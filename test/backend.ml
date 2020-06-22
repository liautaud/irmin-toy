(** Suite of tests for generic backends.

    These tests are meant to check backends independently from the database
    implementation. Since the types of branches, commits, nodes and blobs are
    "opaque" to the backend, we don't need to use the types that the database
    would usually provide, and we can simply use strings instead. *)

open Shared

let hash = String.hash

let check_hashes message target current =
  Alcotest.(check (option string))
    message
    (Option.map (fun x -> String.Digest.to_hex (hash x)) target)
    (Option.map String.Digest.to_hex current)

module type CA =
  Toy.S.CONTENT_ADDRESSABLE_STORE
    with type key = String.Digest.t
     and type value = String.t

let make :
    type c.
    (module Toy.S.BACKEND with type config = c) ->
    config:(unit -> c) ->
    name:string ->
    unit Alcotest_lwt.test =
 fun (module Backend) ~config ~name ->
  let module Backend = Backend.Make (String) (String) (String) (String) in
  let module Branches = Backend.Branches in
  let create () = Backend.create (config ()) in

  let run f =
    let t = create () in
    let%lwt () = f t in
    Backend.close t
  in

  let create_without_raising () = run (fun _ -> Lwt.return_unit) in

  let set_branches () =
    run (fun backend ->
        let t = Backend.branches_t backend in
        let%lwt () = Branches.set t "a" (hash "b") in
        let%lwt () = Branches.set t "b" (hash "c") in
        let%lwt () = Branches.set t "a" (hash "d") in
        let%lwt mem_a = Branches.mem t "a" in
        let%lwt mem_b = Branches.mem t "b" in
        let%lwt mem_c = Branches.mem t "c" in
        let%lwt opt_a = Branches.find t "a" in
        let%lwt opt_b = Branches.find t "b" in
        let%lwt opt_c = Branches.find t "c" in

        Alcotest.(check bool) "Presence of a" true mem_a;
        Alcotest.(check bool) "Presence of b" true mem_b;
        Alcotest.(check bool) "Presence of c" false mem_c;
        check_hashes "Retrieval of a" (Some "d") opt_a;
        check_hashes "Retrieval of b" (Some "c") opt_b;
        check_hashes "Retrieval of c" None opt_c;
        Lwt.return_unit)
  in

  let test_and_set_branches () =
    run (fun backend ->
        let t = Backend.branches_t backend in
        let%lwt ts_1 =
          Branches.test_and_set t "a" ~test:None ~set:(Some (hash "a"))
        in
        let%lwt opt_1 = Branches.find t "a" in
        let%lwt ts_2 =
          Branches.test_and_set t "a"
            ~test:(Some (hash "b"))
            ~set:(Some (hash "c"))
        in
        let%lwt opt_2 = Branches.find t "a" in
        let%lwt ts_3 =
          Branches.test_and_set t "a"
            ~test:(Some (hash "a"))
            ~set:(Some (hash "d"))
        in
        let%lwt opt_3 = Branches.find t "a" in
        let%lwt ts_4 =
          Branches.test_and_set t "b"
            ~test:(Some (hash "a"))
            ~set:(Some (hash "a"))
        in
        let%lwt opt_4 = Branches.find t "b" in
        let%lwt ts_5 =
          Branches.test_and_set t "b" ~test:None ~set:(Some (hash "b"))
        in
        let%lwt opt_5 = Branches.find t "b" in
        let%lwt ts_6 =
          Branches.test_and_set t "b" ~test:(Some (hash "b")) ~set:None
        in
        let%lwt opt_6 = Branches.find t "b" in

        Alcotest.(check bool) "Test and set - 1" true ts_1;
        Alcotest.(check bool) "Test and set - 2" false ts_2;
        Alcotest.(check bool) "Test and set - 3" true ts_3;
        Alcotest.(check bool) "Test and set - 4" false ts_4;
        Alcotest.(check bool) "Test and set - 5" true ts_5;
        Alcotest.(check bool) "Test and set - 6" true ts_6;
        check_hashes "Retrieval - 1" (Some "a") opt_1;
        check_hashes "Retrieval - 2" (Some "a") opt_2;
        check_hashes "Retrieval - 3" (Some "d") opt_3;
        check_hashes "Retrieval - 4" None opt_4;
        check_hashes "Retrieval - 5" (Some "b") opt_5;
        check_hashes "Retrieval - 6" None opt_6;
        Lwt.return_unit)
  in

  let delete_branches () =
    run (fun backend ->
        let t = Backend.branches_t backend in
        let%lwt () = Branches.set t "a" (hash "b") in
        let%lwt mem_1 = Branches.mem t "a" in
        let%lwt () = Branches.remove t "a" in
        let%lwt mem_2 = Branches.mem t "a" in

        Alcotest.(check bool) "Branch is present" true mem_1;
        Alcotest.(check bool) "Branch is deleted" false mem_2;
        Lwt.return_unit)
  in

  let list_branches () =
    run (fun backend ->
        let t = Backend.branches_t backend in
        let%lwt list_1 = Branches.list t in
        let%lwt () = Branches.set t "a" (hash "") in
        let%lwt list_2 = Branches.list t in
        (print_endline @@ Irmin.Type.(to_string (list string) list_2));
        let%lwt () = Branches.set t "b" (hash "") in
        let%lwt () = Branches.set t "c" (hash "") in
        let%lwt list_3 = Branches.list t in

        check_unordered_lists "Empty list" [] list_1;
        check_unordered_lists "Single element" [ "a" ] list_2;
        check_unordered_lists "Multiple elements" [ "a"; "b"; "c" ] list_3;
        Lwt.return_unit)
  in

  let set_generic :
      type a. (module CA with type t = a) -> (Backend.t -> a) -> unit Lwt.t =
   fun (module Store) store_t ->
    run (fun backend ->
        let t = store_t backend in
        let%lwt k_a = Store.set t "a" in
        let%lwt k_b = Store.set t "b" in
        let%lwt mem_a = Store.mem t k_a in
        let%lwt mem_b = Store.mem t k_b in
        let%lwt mem_c = Store.mem t (hash "c") in
        let%lwt opt_a = Store.find t k_a in
        let%lwt opt_b = Store.find t k_b in

        Alcotest.(check bool) "Presence of a" true mem_a;
        Alcotest.(check bool) "Presence of b" true mem_b;
        Alcotest.(check bool) "Absence of c" false mem_c;
        Alcotest.(check (option string)) "Retrieval of a" (Some "a") opt_a;
        Alcotest.(check (option string)) "Retrieval of b" (Some "b") opt_b;
        Lwt.return_unit)
  in

  let filter_generic :
      type a. (module CA with type t = a) -> (Backend.t -> a) -> unit Lwt.t =
   fun (module Store) store_t ->
    run (fun backend ->
        let t = store_t backend in
        let%lwt k_a = Store.set t "a" in
        let%lwt k_b = Store.set t "b" in
        let%lwt k_c = Store.set t "c" in
        let%lwt k_d = Store.set t "d" in
        let%lwt k_e = Store.set t "e" in
        let%lwt () = Store.filter t (fun k -> List.mem k [ k_a; k_c; k_e ]) in
        let%lwt mem_a = Store.mem t k_a in
        let%lwt mem_b = Store.mem t k_b in
        let%lwt mem_c = Store.mem t k_c in
        let%lwt mem_d = Store.mem t k_d in
        let%lwt mem_e = Store.mem t k_e in
        Alcotest.(check bool) "Presence of a" true mem_a;
        Alcotest.(check bool) "Absence of b" false mem_b;
        Alcotest.(check bool) "Presence of c" true mem_c;
        Alcotest.(check bool) "Absence of d" false mem_d;
        Alcotest.(check bool) "Presence of e" true mem_e;

        let%lwt k_b = Store.set t "b" in
        let%lwt k_e = Store.set t "e" in
        let%lwt k_f = Store.set t "f" in
        let%lwt () = Store.filter t (fun k -> List.mem k [ k_a; k_b; k_f ]) in
        let%lwt mem_a = Store.mem t k_a in
        let%lwt mem_b = Store.mem t k_b in
        let%lwt mem_c = Store.mem t k_c in
        let%lwt mem_d = Store.mem t k_d in
        let%lwt mem_e = Store.mem t k_e in
        let%lwt mem_f = Store.mem t k_f in
        Alcotest.(check bool) "Presence of a" true mem_a;
        Alcotest.(check bool) "Presence of b" true mem_b;
        Alcotest.(check bool) "Absence of c" false mem_c;
        Alcotest.(check bool) "Absence of d" false mem_d;
        Alcotest.(check bool) "Absence of e" false mem_e;
        Alcotest.(check bool) "Presence of f" true mem_f;

        Lwt.return_unit)
  in

  let set_commits () = set_generic (module Backend.Commits) Backend.commits_t in

  let filter_commits () =
    filter_generic (module Backend.Commits) Backend.commits_t
  in

  let set_nodes () = set_generic (module Backend.Nodes) Backend.nodes_t in

  let filter_nodes () = filter_generic (module Backend.Nodes) Backend.nodes_t in

  let set_blobs () = set_generic (module Backend.Blobs) Backend.blobs_t in

  let filter_blobs () = filter_generic (module Backend.Blobs) Backend.blobs_t in

  ( name,
    [
      ("create_without_raising", `Quick, create_without_raising);
      ("set_branches", `Quick, set_branches);
      ("test_and_set_branches", `Quick, test_and_set_branches);
      ("delete_branches", `Quick, delete_branches);
      ("list_branches", `Quick, list_branches);
      ("set_commits", `Quick, set_commits);
      ("filter_commits", `Quick, filter_commits);
      ("set_nodes", `Quick, set_nodes);
      ("filter_nodes", `Quick, filter_nodes);
      ("set_blobs", `Quick, set_blobs);
      ("filter_blobs", `Quick, filter_blobs);
    ] )
