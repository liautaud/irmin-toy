module Memory_DB = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Memory)
(** Database modules with different backends. *)

(* module Copy_DB = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Block.Copy) *)
(* module Lazy_DB = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Block.Lazy) *)

(** Returns an ASCII character at random. *)
let random_char () = Char.chr (97 + Random.int 26)

(** Returns a byte-string of the given length at random. *)
let random_bytes n =
  let buf = Buffer.create n in
  for _ = 1 to n do
    Buffer.add_char buf (random_char ())
  done;
  Buffer.to_bytes buf

(** Benchmarks of the tracing algorithm. *)
let bench_tracing () =
  (* Create 50 000 commits which replace the value of a single path with a
     (almost-)unique random string of 16 bytes. This should generate an object
     graph with around [50 000 * 3 = 150 000] objects (one for each commit, node
     and unique blob created). *)
  let n = 10_000 in
  let t = Memory_DB.create () in
  let%lwt () = Memory_DB.initialize t ~master:"master" in
  let%lwt w = Memory_DB.checkout t "master" in
  let rec generate = function
    | 0 -> Lwt.return_unit
    | i ->
        let bytes = random_bytes 16 in
        let blob = Bytes.to_string bytes in
        let%lwt _ = Memory_DB.set w [ "a" ] blob in
        generate (i - 1)
  in

  Logs.info (fun l -> l "Generating %d commits." n);
  let%lwt () = generate n in
  let%lwt () = Memory_DB.Graph.cleanup ~entry:`Branches t in
  Memory_DB.close t

let () =
  Logs.set_level ~all:true (Some Logs.Info);
  Logs.set_reporter (Logs_fmt.reporter ());
  Lwt_main.run @@ bench_tracing ()
