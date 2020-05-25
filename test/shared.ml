module String = Toy.Type.String
module Hashed = Toy.Hashable (Toy.Hash.SHA256) (String)

let run = Lwt_main.run

let print_list = Irmin.Type.(to_string (list string))

let check_unordered_lists msg a b =
  List.iter (fun e -> Alcotest.(check bool) msg true (List.mem e b)) a;
  List.iter (fun e -> Alcotest.(check bool) msg true (List.mem e a)) b
