(** First-fits block backend for irmin-toy.

    This backend stores all branches, commits, nodes and blobs into large pack
    files on the filesystem. The backend is configured by passing the path to
    the root folder used for storage.

    When removing objects from the backend, the space is reclaimed by adding it
    to a free-list, which is then reused by following calls to [set]. When there
    is too much fragmentation, the pack can be compacted by copying all the live
    objects and their entries in the index to a new pack and index. *)

open Lwt.Infix

let src = Logs.Src.create "backend.block.lazy" ~doc:"Lazy block backend"

module Log = (val Logs.src_log src : Logs.LOG)

let ( /> ) = Filename.concat

let ( ++ ) = Int64.add

let ( -- ) = Int64.sub

let ( // ) x y = Int64.to_float x /. Int64.to_float y

module Pack = Unix_pack

let version = "00000001"

(** The next run of [filter] will be compacting when
    [reclaimed_count > reclaimed_threshold]. *)
let reclaimed_threshold = 10_000L

(** The next run of [filter] will be compacting when
    [used_pack_size / total_pack_size < loss_ration_threshold] *)
let loss_ratio_threshold = 0.8

(** Minimum number of bytes to split a reclaimed block into two. *)
let split_threshold = 8

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
    (* Mutable set of (offset, size) in the pack that have been reclaimed. *)
    mutable reclaimed : (int64 * int) list;
    mutable reclaimed_count : int64;
    mutable used_pack_size : int64;
    mutable total_pack_size : int64;
    mutable needs_compact : bool;
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
    Log.info (fun l -> l "Creating at path %s." path);
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
      reclaimed = [];
      reclaimed_count = 0L;
      used_pack_size = 0L;
      total_pack_size = 0L;
      needs_compact =
        true (* Begin with a compaction run to mitigate dirty stops. *);
    }

  let close t =
    Index.close t.index_a;
    Index.close t.index_b;
    Pack.Unix.close t.pack_a;
    Pack.Unix.close t.pack_b;
    Lwt.return_unit

  let mem t k =
    let index = get_current_index t in
    Lwt.return
      ( match Index.find index k with
      (* The offset is -1 when the object has been reclaimed. *)
      | -1L, _ -> false
      (* The object doesn't exist. *)
      | exception Not_found -> false
      | _ -> true )

  let find t k =
    let index = get_current_index t in
    let pack = get_current_pack t in
    let found =
      match Index.find index k with
      | -1L, _ ->
          None (* The offset is -1 when the object has been reclaimed. *)
      | offset, size ->
          let buffer = Bytes.create size in
          let read = Pack.Unix.read pack ~off:offset buffer in
          if read < size then None
          else
            Bytes.unsafe_to_string buffer |> V.unserialize |> Result.to_option
      | exception Not_found -> None
    in
    Lwt.return found

  (** Finds an offset in the pack at which to insert an object of some [size].

      - Starts by looking for space in the list of previously [reclaimed]
        offsets. If such an offset exists, returns [`Offset o]. Note that this
        operation can mutate the [reclaimed] list, e.g. to split a larger block
        of reclaimed memory into two blocks.
      - If no such space is available, returns [`Append] instead. *)
  let unsafe_allocate t requested_size =
    let first_fits = ref None in
    let find_first_fits (offset, size) =
      if Option.is_some !first_fits then Some (offset, size)
      else if size >= requested_size then (
        first_fits := Some offset;
        if size - requested_size > split_threshold then
          Some (offset ++ Int64.of_int requested_size, size - requested_size)
        else (
          t.reclaimed_count <- t.reclaimed_count -- 1L;
          None ) )
      else Some (offset, size)
    in

    (* Find the first reclaimed region of the pack whose size is sufficient to
       fit the new object, and update the list of reclaimed regions to allow
       splitting regions when necessary. *)
    t.reclaimed <- List.filter_map find_first_fits t.reclaimed;
    t.used_pack_size <- t.used_pack_size ++ Int64.of_int requested_size;
    match !first_fits with
    | None ->
        t.total_pack_size <- t.total_pack_size ++ Int64.of_int requested_size;
        `Append
    | Some offset -> `Offset offset

  let unsafe_set t k v =
    Log.info (fun l -> l "Setting [%s] to [%s]." (K.to_hex k) (V.serialize v));
    let index = get_current_index t in
    let pack = get_current_pack t in
    let offset = Pack.Unix.offset pack in
    let serialized = V.serialize v in
    let size = String.length serialized in
    ( match unsafe_allocate t size with
    | `Offset off -> Pack.Unix.set pack ~off serialized
    | `Append ->
        Pack.Unix.append pack serialized;
        Pack.Unix.sync pack;
        Index.replace index k (offset, size) );
    Lwt.return_unit

  let set t k v =
    Log.info (fun l -> l "Waiting for the write_lock.");
    Lwt_mutex.with_lock t.write_lock (fun () -> unsafe_set t k v)

  let list t =
    let index = get_current_index t in
    let keys = ref [] in
    Index.iter (fun k _ -> keys := k :: !keys) index;
    Lwt.return !keys

  (** Reclaims the pack storage at [offset..offset+size] used by the object [k]. *)
  let unsafe_reclaim t k ~offset ~size =
    let index = get_current_index t in
    Index.replace index k (-1L, size);
    (* FIXME(liautaud): Terrible memory layout, use a vector instead. *)
    t.reclaimed <- (offset, size) :: t.reclaimed;
    t.reclaimed_count <- t.reclaimed_count ++ 1L;
    t.used_pack_size <- t.used_pack_size -- Int64.of_int size

  (** Runs a lazy pass of [filter]. *)
  let unsafe_filter t p =
    (* Iterate over all the entries in the current index, and reclaim all those which
       don't need to be kept according to the predicate [p]. *)
    Log.info (fun l -> l "Starting lazy filtering.");
    let index = get_current_index t in
    let iter k (offset, size) =
      (* Only reclaim entries which haven't been previously reclaimed (which have an
         offset of -1) and which don't satisfy the predicate [p]. *)
      if offset > -1L && not (p k) then unsafe_reclaim t k ~offset ~size
    in
    Index.iter iter index;
    Lwt.return_unit

  (** Runs a compaction pass of [filter]. *)
  let unsafe_compact t p =
    (* Iterate over all the entries in the current index, and copy all the entries
       that need to be kept to the other index and other pack. Once they are all
       copied, atomically switch [t.current], and finally erase the contents of
       the previous index and pack. *)
    Log.info (fun l -> l "Starting compact filtering.");
    let current_index = get_current_index t in
    let other_index = get_other_index t in
    let current_pack = get_current_pack t in
    let other_pack = get_other_pack t in
    let new_pack_size = ref 0L in
    let iter k (old_offset, size) =
      (* Only copy entries which haven't been previously reclaimed (which have an
         offset of -1) and which satisfy the predicate [p]. *)
      if old_offset > -1L && p k then (
        let buffer = Bytes.create size in
        let read = Pack.Unix.read current_pack ~off:old_offset buffer in
        if read < size then invalid_arg "No bytes read."
        else
          let new_offset = Pack.Unix.offset other_pack in
          Pack.Unix.append other_pack (Bytes.unsafe_to_string buffer);
          new_pack_size := !new_pack_size ++ Int64.of_int size;
          Index.replace other_index k (new_offset, size) )
    in
    Index.iter iter current_index;
    Pack.Unix.sync other_pack;
    t.reclaimed <- [];
    t.reclaimed_count <- 0L;
    t.used_pack_size <- !new_pack_size;
    t.total_pack_size <- !new_pack_size;
    t.needs_compact <- false;
    set_current t (match t.current with `A -> `B | `B -> `A);
    Lwt_unix.yield () >>= fun () ->
    Index.clear current_index;
    Pack.Unix.clear current_pack;
    Lwt.return_unit

  let filter t p =
    (* This guarded section prevents calls to filter from running concurrently.
       It is only needed because of the [Lwt_unix.yield ()] after the current
       pack is switched, as this introduces the possibility that a new call to
       [filter] could start without the previous index and pack having been
       cleared properly. Note that this _doesn't_ prevent other calls (e.g. to
       [find] or [set]) from running since the critical section (iterating over
       the index) isn't interrupted by a cooperation point. *)
    Log.info (fun l -> l "Waiting for the filter_lock.");
    Lwt_mutex.with_lock t.filter_lock (fun () ->
        if
          t.needs_compact
          || t.reclaimed_count > reclaimed_threshold
          || t.used_pack_size // t.total_pack_size < loss_ratio_threshold
        then unsafe_compact t p
        else unsafe_filter t p)

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
