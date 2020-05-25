open Shared
open Lwt.Infix
module Database = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Memory)

let dump t =
  let buffer = Buffer.create 256 in
  let%lwt () =
    Database.Graph.export ~full:true ~min:[] ~max:[ `Branch "master" ]
      ~name:"Test" t buffer
  in
  Buffer.output_buffer stderr buffer;
  Lwt.return_unit

let make () =
  let t = Database.create () in
  let%lwt () = Database.initialize ~master:"master" t in
  Lwt.return t

let check_mem w p b =
  let message = Printf.sprintf "Existence of %s" (print_list p) in
  let%lwt mem = Database.mem w p in
  Alcotest.(check bool) message b mem;
  Lwt.return_unit

let create_and_initialize () = run (make () >|= ignore)

let checkout_branch () =
  run
  @@ let%lwt t = make () in
     let%lwt _ = Database.checkout t "master" in
     Lwt.return_unit

let detach_commit () =
  run
  @@ let%lwt t = make () in
     let%lwt c = Database.Branches.get t "master" in
     let ch = Database.Commits.to_hash c in
     let%lwt _ = Database.detach t ch in
     Lwt.return_unit

let set_remove_and_get () =
  run
  @@ let%lwt t = make () in
     let%lwt w = Database.checkout t "master" in
     let%lwt c_1 = Database.set ~message:"Message 1." w [ "a" ] "foo" in
     let%lwt b_1 = Database.get w [ "a" ] in
     Alcotest.(check string) "Retrieve a" "foo" b_1;
     let%lwt c_2 = Database.set ~message:"Message 2." w [ "a" ] "bar" in
     let%lwt b_2 = Database.get w [ "a" ] in
     Alcotest.(check string) "Retrieve a again" "bar" b_2;
     let%lwt c_3 = Database.set ~message:"Message 3." w [ "b" ] "baz" in
     let%lwt b_3 = Database.get w [ "b" ] in
     Alcotest.(check string) "Retrieve b" "baz" b_3;
     let%lwt c_4 = Database.remove ~message:"Message 4." w [ "a" ] in
     let%lwt mem_a = Database.mem w [ "a" ] in
     Alcotest.(check bool) "Presence of a" false mem_a;

     let m_1 = Database.Commits.message c_1 in
     Alcotest.(check string) "Commit message 1" "Message 1." m_1;
     let m_2 = Database.Commits.message c_2 in
     Alcotest.(check string) "Commit message 2" "Message 2." m_2;
     let m_3 = Database.Commits.message c_3 in
     Alcotest.(check string) "Commit message 3." "Message 3." m_3;
     let m_4 = Database.Commits.message c_4 in
     Alcotest.(check string) "Commit message 4." "Message 4." m_4;
     Lwt.return_unit

let set_tree_merge () =
  let tree_1 = `Node [ ("b", `Blob "b"); ("c", `Node [ ("d", `Blob "d") ]) ] in
  let tree_2 =
    `Node
      [
        ("b", `Node [ ("e", `Blob "e") ]);
        ("c", `Node [ ("f", `Blob "f") ]);
        ("g", `Blob "g");
      ]
  in

  run
  @@ let%lwt t = make () in
     let%lwt w = Database.checkout t "master" in
     let%lwt _ = Database.set_tree ~message:"" ~mode:`Merge w [ "a" ] tree_1 in
     let%lwt () = check_mem w [ "a"; "b" ] true in
     let%lwt () = check_mem w [ "a"; "c" ] true in
     let%lwt () = check_mem w [ "a"; "c"; "d" ] true in
     let%lwt () = check_mem w [ "a"; "d" ] false in
     let%lwt _ = Database.set_tree ~message:"" ~mode:`Merge w [ "a" ] tree_2 in
     let%lwt () = check_mem w [ "a"; "b" ] true in
     let%lwt () = check_mem w [ "a"; "b"; "e" ] true in
     let%lwt () = check_mem w [ "a"; "c" ] true in
     let%lwt () = check_mem w [ "a"; "c"; "d" ] true in
     let%lwt () = check_mem w [ "a"; "c"; "f" ] true in
     let%lwt () = check_mem w [ "a"; "g" ] true in
     Lwt.return_unit

let set_tree_override () =
  let tree_1 = `Node [ ("b", `Blob "b"); ("c", `Node [ ("d", `Blob "d") ]) ] in
  let tree_2 =
    `Node
      [
        ("b", `Node [ ("e", `Blob "e") ]);
        ("c", `Node [ ("f", `Blob "f") ]);
        ("g", `Blob "g");
      ]
  in

  run
  @@ let%lwt t = make () in
     let%lwt w = Database.checkout t "master" in
     let%lwt _ =
       Database.set_tree ~message:"" ~mode:`Override w [ "a" ] tree_1
     in
     let%lwt () = check_mem w [ "a"; "b" ] true in
     let%lwt () = check_mem w [ "a"; "c" ] true in
     let%lwt () = check_mem w [ "a"; "c"; "d" ] true in
     let%lwt () = check_mem w [ "a"; "d" ] false in
     let%lwt _ =
       Database.set_tree ~message:"" ~mode:`Override w [ "a" ] tree_2
     in
     let%lwt () = check_mem w [ "a"; "b" ] true in
     let%lwt () = check_mem w [ "a"; "b"; "e" ] true in
     let%lwt () = check_mem w [ "a"; "c" ] true in
     let%lwt () = check_mem w [ "a"; "c"; "d" ] false in
     let%lwt () = check_mem w [ "a"; "c"; "f" ] true in
     let%lwt () = check_mem w [ "a"; "g" ] true in
     Lwt.return_unit

let suite =
  ( "database",
    [
      ("create_and_initialize", `Quick, create_and_initialize);
      ("checkout_branch", `Quick, checkout_branch);
      ("detach_commit", `Quick, detach_commit);
      ("set_remove_and_get", `Quick, set_remove_and_get);
      ("set_tree_merge", `Quick, set_tree_merge);
      ("set_tree_override", `Quick, set_tree_override);
    ] )
