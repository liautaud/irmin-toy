(* module DB = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Memory) *)
module DB = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Block.Copy)
(* module DB = Toy.Basic (Toy.Hash.SHA256) (Toy.Backend.Block.Lazy) *)

(** Displays a Graphviz dump of the object graph. *)
let dump t =
  let buffer = Buffer.create 256 in
  let%lwt () =
    DB.Graph.export ~full:true ~min:[] ~max:[ `Branch "master" ]
      ~name:"Test" t buffer
  in
  Buffer.output_buffer stderr buffer;
  Lwt.return_unit

(** Returns a printable ASCII character at random. *)
let random_char () = Char.chr (97 + Random.int 26)

(** Returns a byte-string of the given length at random. *)
let random_bytes n =
  let buf = Buffer.create n in
  for _ = 1 to n do
    Buffer.add_char buf (random_char ())
  done;
  Buffer.to_bytes buf

(** Returns the path to a new temporary directory. *)
let new_tmp_directory () =
  let dir =
    Bos.OS.Dir.tmp "irmin_bench_%s" |> Result.get_ok |> Fpath.to_string
  in
  Logs.info (fun l -> l "Using temporary directory: %s." dir);
  dir

(** Benchmarks of the tracing algorithm. *)
let bench_tracing () =
  (* Create 1 000 000 commits which replace the value of a single path with a
     (almost-)unique random string of 16 bytes. This should generate an object
     graph with around 3 000 000 objects (one for each commit, node and blob). *)
  let n = 1_000_000 in
  let t = DB.create (new_tmp_directory ()) in
  let%lwt () = DB.initialize t ~master:"master" in
  let%lwt w = DB.checkout t "master" in
  let rec generate = function
    | 0 -> Lwt.return_unit
    | i ->
        let bytes = random_bytes 16 in
        let blob = Bytes.to_string bytes in
        let%lwt _ = DB.set w [ "a" ] blob in
        generate (i - 1)
  in

  Logs.info (fun l -> l "Generating %d commits." n);
  let%lwt () = generate n in
  (* let%lwt () = dump t in *)
  let start = Sys.time () in
  Logs.debug (fun l -> l "Starting the collection at %f." start);
  (* let%lwt status = DB.Graph.cleanup ~heads:`Branches t ~limit_time:1. in *)
  let%lwt status = DB.Graph.cleanup ~heads:`Branches t in
  let () =
    match status with
    | Ok () -> Logs.info (fun l -> l "Collection ended with Ok ().")
    | Error `Run_again ->
        Logs.info (fun l -> l "Collection ended with `Run_again.")
  in
  let stop = Sys.time () in
  Logs.debug (fun l -> l "Ending the collection at %f." stop);
  Logs.info (fun l -> l "Total duration: %f." (stop -. start));
  DB.close t

let () =
  Logs.set_level ~all:true (Some Logs.App);
  Logs.Src.set_level Logs.default (Some Logs.Debug);
  Logs.Src.set_level Toy.src (Some Logs.Info);
  (* Logs.Src.set_level Toy.Backend.Block.Copy.src (Some Logs.Info); *)

  Logs.set_reporter (Logs_fmt.reporter ());
  Lwt_main.run @@ bench_tracing ()
