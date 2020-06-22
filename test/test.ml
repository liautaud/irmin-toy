let new_tmp_directory () =
  let dir =
    Bos.OS.Dir.tmp "irmin_test_%s" |> Result.get_ok |> Fpath.to_string
  in
  Logs.info (fun l -> l "Using temporary directory: %s." dir);
  dir

(** Tests of general database operations. *)
let database = Database.suite

(** Tests of the tracing garbage collector. *)
let tracing = Tracing.suite

(** Tests of the in-memory backend. *)
let memory =
  Backend.make (module Toy.Backend.Memory) ~config:(fun () -> ()) ~name:"memory"

(** Tests of the filesystem backend. *)
let filesystem =
  Backend.make
    (module Toy.Backend.Filesystem)
    ~config:new_tmp_directory ~name:"filesystem"

(** Tests of the copying block backend. *)
let block_copy =
  Backend.make
    (module Toy.Backend.Block.Copy)
    ~config:new_tmp_directory ~name:"block-copy"

(** Tests of the lazy block backend. *)
let block_lazy =
  Backend.make
    (module Toy.Backend.Block.Lazy)
    ~config:new_tmp_directory ~name:"block-lazy"

let suite = [ database; tracing; memory; filesystem; block_copy; block_lazy ]

let () =
  Logs.set_level ~all:true (Some Logs.Debug);
  Logs.set_reporter (Logs_fmt.reporter ());
  Lwt_main.run @@ Alcotest_lwt.run "irmin-toy" suite
