open Lwt.Infix

(* Immutable store which uses in-memory hashtables. *)
module Immutable (K : S.SERIALIZABLE) (V : S.SERIALIZABLE) = struct
  type t = { table : (K.t, V.t) Hashtbl.t }

  type key = K.t

  type value = V.t

  let create () = { table = Hashtbl.create 256 }

  let mem t k = Hashtbl.mem t.table k |> Lwt.return

  let find t k = Hashtbl.find_opt t.table k |> Lwt.return

  let set t k v = Hashtbl.replace t.table k v |> Lwt.return

  let list t = Hashtbl.fold (fun k _ acc -> k :: acc) t.table [] |> Lwt.return

  let remove t k = Hashtbl.remove t.table k |> Lwt.return

  let filter t p =
    list t
    >>= Lwt_list.iter_p (fun k -> if p k then Lwt.return_unit else remove t k)
end

(* Content-addressable store derived from the immutable store. *)
module Content_Addressable (V : S.HASHABLE) = struct
  include Immutable (V.Hash) (V)

  let set t v =
    let h = V.hash v in
    let%lwt () = set t h v in
    Lwt.return h
end

(* Atomically writable store which uses in-memory hashtables. *)
module Atomic_Write (K : S.SERIALIZABLE) (V : S.SERIALIZABLE) = struct
  include Immutable (K) (V)

  let test_and_set t k ~test ~set =
    let update () =
      match set with
      | None -> Hashtbl.remove t.table k
      | Some v -> Hashtbl.add t.table k v
    in
    (* We can't reuse the find method here because Lwt doesn't guarantee
       atomicity across binds, so we would have to use some form of locking.
       Instead, we just use Hashtbl.find_opt to avoid having to bind. *)
    match (Hashtbl.find_opt t.table k, test) with
    | None, None ->
        update ();
        Lwt.return_true
    | Some test, Some v when V.equal test v ->
        update ();
        Lwt.return_true
    | _ -> Lwt.return_false
end

(* Memory backend using the stores defined above. *)
module Memory
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
    branches : Branches.t;
    commits : Commits.t;
    nodes : Nodes.t;
    blobs : Blobs.t;
  }

  type config = unit

  let create () =
    {
      branches = Branches.create ();
      commits = Commits.create ();
      nodes = Nodes.create ();
      blobs = Blobs.create ();
    }

  let branches_t t = t.branches

  let commits_t t = t.commits

  let nodes_t t = t.nodes

  let blobs_t t = t.blobs
end
