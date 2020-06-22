open Toy

module String = Toy.Hashable (Hash.SHA256) ((val ~:Irmin.Type.string))

let print_list = Irmin.Type.(to_string (list string))

let check_unordered_lists msg a b =
  List.iter (fun e -> Alcotest.(check bool) msg true (List.mem e b)) a;
  List.iter (fun e -> Alcotest.(check bool) msg true (List.mem e a)) b
