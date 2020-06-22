(** Filesystem backend for irmin-toy.

    This backend stores all branches, commits, nodes and blobs into separate
    files on separate folders of the host filesystem. The backend is configured
    by passing the path to the root folder used for storage. *)

open Lwt.Infix

let ( / ) = Filename.concat

(* Input-output operations on a Unix filesystem.
   (see https://github.com/mirage/irmin/blob/master/src/irmin-unix/fs.ml). *)
module IO = Unix_fs.IO

(* Types that can be sanitized into a path-safe string representation.
   This is required because some characters are forbidden in filesystem paths. *)
module type SANITIZABLE = sig
  include S.SERIALIZABLE

  val sanitize : t -> string

  val unsanitize : string -> (t, [ `Msg of string ]) result
end

(* Base64 encoding and decoding of strings. *)
module Base64 (K : S.SERIALIZABLE) : SANITIZABLE with type t = K.t = struct
  include K

  let sanitize h = Base64.encode_exn (K.serialize h)

  let unsanitize h = Result.bind (Base64.decode h) K.unserialize
end

(* Immutable store which uses the host filesystem. *)
module Immutable (K : SANITIZABLE) (V : S.SERIALIZABLE) = struct
  type t = { path : string }

  type key = K.t

  type value = V.t

  (** Name of the file used as a write-lock by the IO module. *)
  let lock = ".lock"

  let create path = { path }

  let close _t = Lwt.return_unit

  (** [path t k] is the path of the file which stores the contents of [k]. *)
  let path t k = t.path / K.sanitize k

  let mem t k = IO.file_exists (path t k)

  let find t k =
    try%lwt
      IO.read_file (path t k) >|= function
      | None -> None
      | Some b -> Result.to_option (V.unserialize b)
    with _ -> Lwt.return_none

  let set t k v = IO.write_file ~lock (path t k) (V.serialize v)

  let list t =
    IO.files t.path >|= List.map Filename.basename
    >|= List.filter_map (function
          | ".lock" -> None
          | k -> Result.to_option (K.unsanitize k))

  let filter t p =
    IO.Lock.with_lock (Some lock) (fun () ->
        list t
        >|= List.filter (fun k -> not (p k))
        >|= List.map (path t)
        >>= Lwt_list.iter_p IO.remove_file)

  let remove t k = IO.remove_file ~lock (path t k)

  let test_and_set t k ~test ~set =
    let test = Option.map V.serialize test in
    let set = Option.map V.serialize set in
    IO.test_and_set_file ~lock ~test ~set (path t k)
end

(* Content-addressable store derived from the immutable store. *)
module Content_Addressable (V : S.HASHABLE) = struct
  module Digest = struct
    include V.Digest

    let sanitize = V.Digest.to_hex

    let unsanitize = V.Digest.of_hex
  end

  (* Sanitize the hashes using their hexadecimal representation. *)
  include Immutable (Digest) (V)

  let set t v =
    let h = V.hash v in
    let%lwt () = set t h v in
    Lwt.return h
end

(* Atomically writable store which uses the host filesystem. *)
module Atomic_Write (K : S.SERIALIZABLE) (V : S.SERIALIZABLE) = struct
  (* Sanitize the arbitrary keys using Base64 encoding. *)
  include Immutable (Base64 (K)) (V)
end

(* Filesystem backend using the stores defined above. *)
module Backend = struct
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
        branches = Branches.create (path / "branches");
        commits = Commits.create (path / "commits");
        nodes = Nodes.create (path / "nodes");
        blobs = Blobs.create (path / "blobs");
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
