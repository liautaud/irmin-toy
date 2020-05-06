let ( / ) = Filename.concat

(* Immutable store which uses the host filesystem. *)
module Immutable (K : S.SERIALIZABLE) (V : S.SERIALIZABLE) = struct
  type t = { path : string }

  type key = K.t

  type value = V.t

  let create path = { path }

  let mem t k = failwith "Not implemented."

  let find t k = failwith "Not implemented."

  let set t k v = failwith "Not implemented."

  let filter t p = failwith "Not implemented."
end

(* Content-addressable store derived from the immutable store. *)
module Content_Addressable (V : S.HASHABLE) = struct
  include Immutable (V.Hash) (V)

  let set t v =
    let h = V.hash v in
    let%lwt () = set t h v in
    Lwt.return h
end

(* Atomically writable store which uses the host filesystem. *)
module Atomic_Write (K : S.SERIALIZABLE) (V : S.SERIALIZABLE) = struct
  type t = { path : string }

  type key = K.t

  type value = V.t

  let create path = { path }

  let mem t k = failwith "Not implemented."

  let find t k = failwith "Not implemented."

  let set t k v = failwith "Not implemented."

  let test_and_set t k ~test ~set = failwith "Not implemented."

  let remove t k = failwith "Not implemented."

  let list t = failwith "Not implemented."
end

(* Filesystem backend using the stores defined above. *)
module Filesystem
    (Branch : S.SERIALIZABLE)
    (Commit : S.HASHABLE)
    (Node : S.HASHABLE)
    (Blob : S.HASHABLE) =
struct
  module Branches = Atomic_Write (Branch) (Commit.Hash)
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

  type config = string

  let create path =
    {
      path;
      branches = Branches.create (path / "branches");
      commits = Commits.create (path / "commits");
      nodes = Nodes.create (path / "nodes");
      blobs = Blobs.create (path / "blobs");
    }

  let branches_t t = t.branches

  let commits_t t = t.commits

  let nodes_t t = t.nodes

  let blobs_t t = t.blobs
end
