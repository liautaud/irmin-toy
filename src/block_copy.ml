(** Copying block backend for irmin-toy.

    This backend stores all branches, commits, nodes and blobs into large pack
    files on the filesystem. The backend is configured by passing the path to
    the root folder used for storage.

    When removing objects from the backend, the entire contents of both the
    index and the pack are copied to a new index and a new pack, which are then
    swapped atomically.

    TODO: Add the [batch_filters] option which enables a heuristic to wait for
    enough objects to be deleted before actually copying the contents of the
    index and pack. *)

open Lwt.Infix

let src = Logs.Src.create "backend.block.copy" ~doc:"Copying block backend"

module Log = (val Logs.src_log src : Logs.LOG)

let ( /> ) = Filename.concat

module Pack = Unix_pack

let version = "00000001"

module Index_K (D : S.DIGEST) = struct
  type t = D.t
  (** The index uses the hash digests of the objects as keys. *)

  let pp = Fmt.of_to_string D.to_hex

  let hash = Hashtbl.hash

  let hash_size = 30

  let equal x y = D.equal x y

  let encode x = D.serialize x

  let encoded_size = D.size

  let decode s off =
    String.sub s off encoded_size |> D.unserialize |> Result.get_ok
end

module Index_V = struct
  type t = int64 * int
  (** The index stores (offset, size) references to the pack as values. *)

  let pp = Irmin.Type.(pp (pair int64 int))

  let encode (off, len) =
    Irmin.Type.(to_bin_string (pair int64 int32)) (off, Int32.of_int len)

  let decode s off =
    let off, len = snd (Irmin.Type.(decode_bin (pair int64 int32)) s off) in
    (off, Int32.to_int len)

  let encoded_size = (64 / 8) + (32 / 8)
end

(* Immutable store which uses a single buffer file. *)
module Immutable (K : S.DIGEST) (V : S.SERIALIZABLE) = struct
  type entry = Index_V.t

  module Index = Index_unix.Make (Index_K (K)) (Index_V) (Index.Cache.Noop)

  type t = {
    path : string;
    (* Two atomically switcheable packs for copying garbage collection. *)
    mutable current : [ `A | `B ];
    index_a : Index.t;
    index_b : Index.t;
    pack_a : Pack.Unix.t;
    pack_b : Pack.Unix.t;
    write_lock : Lwt_mutex.t;
    filter_lock : Lwt_mutex.t;
  }

  type key = K.t

  type value = V.t

  (** Reads the value of current from disk. *)
  let read_current path =
    let current_path = path /> "current" in
    try
      let current_chan = Stdlib.open_in current_path in
      let current_char = Stdlib.input_char current_chan in
      Stdlib.close_in current_chan;
      match current_char with 'a' -> `A | 'b' -> `B | _ -> assert false
    with _ -> `A

  (** Persists the value of current to disk. *)
  let persist_current path c =
    let current_path = path /> "current" in
    let current_char = match c with `A -> 'a' | `B -> 'b' in
    let current_chan = Stdlib.open_out current_path in
    Stdlib.output_char current_chan current_char;
    Stdlib.close_out current_chan

  (** Updates the value of t.current and persist it to disk. *)
  let set_current t c =
    persist_current t.path c;
    t.current <- c

  (** Returns the [Pack.Unix.t] corresponding with [t.current]. *)
  let get_current_pack t =
    match t.current with `A -> t.pack_a | `B -> t.pack_b

  (** Returns the [Pack.Unix.t] corresponding with the opposite of [t.current]. *)
  let get_other_pack t = match t.current with `A -> t.pack_b | `B -> t.pack_a

  (** Returns the [Index.t] corresponding with [t.current]. *)
  let get_current_index t =
    match t.current with `A -> t.index_a | `B -> t.index_b

  (** Returns the [Index.t] corresponding with the opposite of [t.current]. *)
  let get_other_index t =
    match t.current with `A -> t.index_b | `B -> t.index_a

  let create path =
    Pack.Unix.mkdir path;
    let current = read_current path in
    (* Persist the value of current in case it wasn't previously stored. *)
    persist_current path current;
    {
      path;
      current;
      index_a =
        Index.v ~fresh:false ~readonly:false ~log_size:500_000
          (path /> "index_a");
      index_b =
        Index.v ~fresh:false ~readonly:false ~log_size:500_000
          (path /> "index_b");
      pack_a =
        Pack.Unix.v ~fresh:false ~version ~readonly:false (path /> "pack_a");
      pack_b =
        Pack.Unix.v ~fresh:false ~version ~readonly:false (path /> "pack_b");
      write_lock = Lwt_mutex.create ();
      filter_lock = Lwt_mutex.create ();
    }

  let close t =
    Index.close t.index_a;
    Index.close t.index_b;
    Pack.Unix.close t.pack_a;
    Pack.Unix.close t.pack_b;
    Lwt.return_unit

  (* FIXME(liautaud): Should fail when the index points to some invalid offset. *)
  let mem t k =
    let index = get_current_index t in
    Lwt.return (Index.mem index k)

  let find t k =
    let index = get_current_index t in
    let pack = get_current_pack t in
    let found =
      match Index.find index k with
      | offset, size ->
          let buffer = Bytes.create size in
          let read = Pack.Unix.read pack ~off:offset buffer in
          if read < size then None
          else
            Bytes.unsafe_to_string buffer |> V.unserialize |> Result.to_option
      | exception Not_found -> None
    in
    Lwt.return found

  let unsafe_set t k v =
    Log.info (fun l -> l "Setting [%s] to [%s]." (K.to_hex k) (V.serialize v));
    let index = get_current_index t in
    let pack = get_current_pack t in
    let offset = Pack.Unix.offset pack in
    let serialized = V.serialize v in
    let size = String.length serialized in
    Pack.Unix.append pack serialized;
    Pack.Unix.sync pack;
    (* FIXME *)
    Index.replace index k (offset, size);
    Lwt.return_unit

  let set t k v =
    Log.info (fun l -> l "Waiting for the write_lock.");

    Lwt_mutex.with_lock t.write_lock (fun () -> unsafe_set t k v)

  let list t =
    let index = get_current_index t in
    let keys = ref [] in
    Index.iter (fun k _ -> keys := k :: !keys) index;
    Lwt.return !keys

  let filter t p =
    (* Iterate over all the entries in the current index, and copy all the entries
       that need to be kept to the other index and other pack. Once they are all
       copied, atomically switch [t.current], and finally erase the contents of
       the previous index and pack. *)
    let start = Sys.time () in
    Log.info (fun l -> l "Waiting for the filter_lock.");
    Lwt_mutex.with_lock t.filter_lock (fun () ->
        (* This guarded section prevents calls to filter from running concurrently.
           It is only needed because of the [Lwt_unix.yield ()] after the current
           pack is switched, as this introduces the possibility that a new call to
           [filter] could start without the previous index and pack having been
           cleared properly. Note that this _doesn't_ prevent other calls (e.g. to
           [find] or [set]) from running since the critical section (iterating over
           the index) isn't interrupted by a cooperation point. *)
        Log.info (fun l -> l "Starting compact filtering.");
        let current_index = get_current_index t in
        let other_index = get_other_index t in
        let current_pack = get_current_pack t in
        let other_pack = get_other_pack t in
        let iter k (old_offset, size) =
          (* Only copy entries which satisfy the predicate [p]. *)
          if p k then (
            let buffer = Bytes.create size in
            let read = Pack.Unix.read current_pack ~off:old_offset buffer in
            if read < size then invalid_arg "No bytes read."
            else
              let new_offset = Pack.Unix.offset other_pack in
              Pack.Unix.append other_pack (Bytes.unsafe_to_string buffer);
              Index.replace other_index k (new_offset, size) )
        in
        (* FIXME(liautaud): Check whether it would make sense to yield at the end of
           each iteration, in which case we would have to prevent calls to [set] from
           running while [filter] is also running with the write_lock ; but the rest
           of the primitives should be able to remain lock-free. *)
        Index.iter iter current_index;
        Pack.Unix.sync other_pack;
        set_current t (match t.current with `A -> `B | `B -> `A);
        Lwt_unix.yield () >>= fun () ->
        Index.clear current_index;
        Pack.Unix.clear current_pack;
        Log.info (fun l -> l "Filter duration: %fs." (Sys.time () -. start));
        Lwt.return_unit)

  let remove t k = filter t (fun kt -> not (K.equal kt k))
end

(* Content-addressable store derived from the immutable store. *)
module Content_Addressable (V : S.HASHABLE) = struct
  include Immutable (V.Digest) (V)

  let set t v =
    let h = V.hash v in
    let%lwt () = set t h v in
    Lwt.return h
end

(* Atomically writable store which uses a single buffer file. *)
module Atomic_Write (K : S.SERIALIZABLE) (V : S.SERIALIZABLE) = struct
  include Filesystem.Atomic_Write (K) (V)
end

(* Block backend using the stores defined above. *)
module Backend = struct
  let src = src

  type config = string

  module Make
      (Branch : S.SERIALIZABLE)
      (Commit : S.HASHABLE)
      (Node : S.HASHABLE)
      (Blob : S.HASHABLE) =
  struct
    module Branches = Atomic_Write (Branch) (Commit.Digest)
    module Commits = Content_Addressable (Commit)
    module Nodes = Content_Addressable (Node)
    module Blobs = Content_Addressable (Blob)

    type t = {
      path : string;
      branches : Branches.t;
      commits : Commits.t;
      nodes : Nodes.t;
      blobs : Blobs.t;
    }

    let create path =
      {
        path;
        branches = Branches.create (path /> "branches");
        commits = Commits.create (path /> "commits");
        nodes = Nodes.create (path /> "nodes");
        blobs = Blobs.create (path /> "blobs");
      }

    let close t =
      let%lwt () = Branches.close t.branches in
      let%lwt () = Commits.close t.commits in
      let%lwt () = Nodes.close t.nodes in
      let%lwt () = Blobs.close t.blobs in
      Lwt.return_unit

    let branches_t t = t.branches

    let commits_t t = t.commits

    let nodes_t t = t.nodes

    let blobs_t t = t.blobs
  end
end
