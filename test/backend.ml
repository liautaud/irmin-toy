open Shared
(** Suite of tests for generic backends.

    These tests are meant to check backends independently from the database
    implementation. Since the types of branches, commits, nodes and blobs are
    "opaque" to the backend, we don't need to use the types that the database
    would usually provide, and we can simply use strings instead. *)

module type CA =
  Toy.S.CONTENT_ADDRESSABLE_STORE with type key = string and type value = string

let make (module Backend : Toy.S.BACKEND) name =
  let module Memory = Toy.Backend.Memory in
  let module Backend = Memory (Hashed) (Hashed) (Hashed) (Hashed) in
  let module Branches = Backend.Branches in
  let create_without_raising () = ignore @@ Backend.create () in

  let set_branches () =
    run
    @@
    let t = Backend.branches_t (Backend.create ()) in
    let%lwt () = Branches.set t "a" "b" in
    let%lwt () = Branches.set t "b" "c" in
    let%lwt () = Branches.set t "a" "d" in
    let%lwt mem_a = Branches.mem t "a" in
    let%lwt mem_b = Branches.mem t "b" in
    let%lwt mem_c = Branches.mem t "c" in
    let%lwt opt_a = Branches.find t "a" in
    let%lwt opt_b = Branches.find t "b" in
    let%lwt opt_c = Branches.find t "c" in

    Alcotest.(check bool) "Presence of a" true mem_a;
    Alcotest.(check (option string)) "Retrieval of a" (Some "d") opt_a;
    Alcotest.(check bool) "Presence of b" true mem_b;
    Alcotest.(check (option string)) "Retrieval of b" (Some "c") opt_b;
    Alcotest.(check bool) "Presence of c" false mem_c;
    Alcotest.(check (option string)) "Retrieval of c" None opt_c;
    Lwt.return_unit
  in

  let test_and_set_branches () =
    run
    @@
    let t = Backend.branches_t (Backend.create ()) in
    let%lwt ts_1 = Branches.test_and_set t "a" ~test:None ~set:(Some "a") in
    let%lwt opt_1 = Branches.find t "a" in
    let%lwt ts_2 =
      Branches.test_and_set t "a" ~test:(Some "b") ~set:(Some "c")
    in
    let%lwt opt_2 = Branches.find t "a" in
    let%lwt ts_3 =
      Branches.test_and_set t "a" ~test:(Some "a") ~set:(Some "d")
    in
    let%lwt opt_3 = Branches.find t "a" in
    let%lwt ts_4 =
      Branches.test_and_set t "b" ~test:(Some "a") ~set:(Some "a")
    in
    let%lwt opt_4 = Branches.find t "b" in
    let%lwt ts_5 = Branches.test_and_set t "b" ~test:None ~set:(Some "b") in
    let%lwt opt_5 = Branches.find t "b" in
    let%lwt ts_6 = Branches.test_and_set t "b" ~test:(Some "b") ~set:None in
    let%lwt opt_6 = Branches.find t "b" in

    Alcotest.(check bool) "Test and set - 1" true ts_1;
    Alcotest.(check (option string)) "Retrieval - 1" (Some "a") opt_1;
    Alcotest.(check bool) "Test and set - 2" false ts_2;
    Alcotest.(check (option string)) "Retrieval - 2" (Some "a") opt_2;
    Alcotest.(check bool) "Test and set - 3" true ts_3;
    Alcotest.(check (option string)) "Retrieval - 3" (Some "d") opt_3;
    Alcotest.(check bool) "Test and set - 4" false ts_4;
    Alcotest.(check (option string)) "Retrieval - 4" None opt_4;
    Alcotest.(check bool) "Test and set - 5" true ts_5;
    Alcotest.(check (option string)) "Retrieval - 5" (Some "b") opt_5;
    Alcotest.(check bool) "Test and set - 6" true ts_6;
    Alcotest.(check (option string)) "Retrieval - 6" None opt_6;
    Lwt.return_unit
  in

  let delete_branches () =
    run
    @@
    let t = Backend.branches_t (Backend.create ()) in
    let%lwt () = Branches.set t "a" "b" in
    let%lwt mem_1 = Branches.mem t "a" in
    let%lwt () = Branches.remove t "a" in
    let%lwt mem_2 = Branches.mem t "a" in

    Alcotest.(check bool) "Branch is present" true mem_1;
    Alcotest.(check bool) "Branch is deleted" false mem_2;
    Lwt.return_unit
  in

  let list_branches () =
    run
    @@
    let t = Backend.branches_t (Backend.create ()) in
    let%lwt list_1 = Branches.list t in
    let%lwt () = Branches.set t "a" "z" in
    let%lwt list_2 = Branches.list t in
    let%lwt () = Branches.set t "b" "z" in
    let%lwt () = Branches.set t "c" "z" in
    let%lwt list_3 = Branches.list t in

    check_unordered_lists "Empty list" [] list_1;
    check_unordered_lists "Single element" [ "a" ] list_2;
    check_unordered_lists "Multiple elements" [ "a"; "b"; "c" ] list_3;
    Lwt.return_unit
  in

  let set_generic :
      type a. (module CA with type t = a) -> (Backend.t -> a) -> unit =
   fun (module Store : CA with type t = a) store_t ->
    run
    @@
    let t = store_t (Backend.create ()) in
    let%lwt k_a = Store.set t "a" in
    let%lwt k_b = Store.set t "b" in
    let%lwt mem_a = Store.mem t k_a in
    let%lwt mem_b = Store.mem t k_b in
    let%lwt mem_c = Store.mem t (Hashed.hash "c") in
    let%lwt opt_a = Store.find t k_a in
    let%lwt opt_b = Store.find t k_b in

    Alcotest.(check bool) "Presence of a" true mem_a;
    Alcotest.(check bool) "Presence of b" true mem_b;
    Alcotest.(check bool) "Absence of c" false mem_c;
    Alcotest.(check (option string)) "Retrieval of a" (Some "a") opt_a;
    Alcotest.(check (option string)) "Retrieval of b" (Some "b") opt_b;
    Lwt.return_unit
  in

  let filter_generic :
      type a. (module CA with type t = a) -> (Backend.t -> a) -> unit =
   fun (module Store : CA with type t = a) store_t ->
    run
    @@
    let t = store_t (Backend.create ()) in
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
    Alcotest.(check bool) "Absence of a" false mem_b;
    Alcotest.(check bool) "Presence of c" true mem_c;
    Alcotest.(check bool) "Absence of d" false mem_d;
    Alcotest.(check bool) "Presence of e" true mem_e;
    Lwt.return_unit
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
