open Lwt.Infix
open Type

let map_fst f = Lwt_list.map_p (fun (a, b) -> f a >|= fun a -> (a, b))

let map_snd f = Lwt_list.map_p (fun (a, b) -> f b >|= fun b -> (a, b))

module Full
    (Hash : S.HASH)
    (Backend : S.BACKEND)
    (Branch : S.SERIALIZABLE)
    (Step : S.SERIALIZABLE)
    (Blob : S.SERIALIZABLE) =
struct
  type branch = Branch.t

  type step = Step.t

  let step_t = Step.t

  type blob = Blob.t

  let blob_t = Blob.t

  type commit_hash = CH of string [@@unboxed] [@@deriving irmin]

  type node_hash = NH of string [@@unboxed] [@@deriving irmin]

  type blob_hash = BH of string [@@unboxed] [@@deriving irmin]

  type node = Blob of blob_hash | Node of (step * node_hash) list
  [@@deriving irmin]

  type commit = {
    parents : commit_hash list;
    node : node_hash;
    message : string;
  }
  [@@deriving irmin]

  module Commit_T = struct
    type t = commit

    let t = commit_t
  end

  module Node_T = struct
    type t = node

    let t = node_t
  end

  module Branch = Hashable (Hash) (Branch)
  module Commit = Hashable_T (Hash) (Commit_T)
  module Node = Hashable_T (Hash) (Node_T)
  module Blob = Hashable (Hash) (Blob)
  module Backend = Backend (Branch) (Commit) (Node) (Blob)

  type config = Backend.config

  type t = { backend : Backend.t; config : config }

  let branches_t t = Backend.branches_t t.backend

  let commits_t t = Backend.commits_t t.backend

  let nodes_t t = Backend.nodes_t t.backend

  let blobs_t t = Backend.blobs_t t.backend

  type workspace = {
    t : t;
    head : [ `Branch of branch | `Head of commit_hash option ref ];
  }

  (** {3 Operations on blobs.} *)

  module Blobs = struct
    let to_hash b = BH (Blob.hash b)

    let of_hash t (BH h) = Backend.Blobs.find (blobs_t t) h >|= Option.get
  end

  (** {3 Operations on nodes.} *)

  module Nodes = struct
    let to_hash n = NH (Node.hash n)

    let of_hash t (NH h) = Backend.Nodes.find (nodes_t t) h >|= Option.get

    let is_blob = function Blob _ -> true | Node _ -> false

    let blob t = function
      | Blob h -> Blobs.of_hash t h
      | Node _ -> invalid_arg "Node is not a leaf."

    let children t = function
      | Blob _ -> invalid_arg "Node is a leaf."
      | Node c -> c |> map_snd (of_hash t)

    let rec tree t n =
      match n with
      | Blob _ -> blob t n >|= fun b -> `Blob b
      | Node _ -> children t n >>= map_snd (tree t) >|= fun n -> `Node n
  end

  (** {3 Operations on commits.} *)

  module Commits = struct
    let to_hash c = CH (Commit.hash c)

    let of_hash t (CH h) = Backend.Commits.find (commits_t t) h >|= Option.get

    let parents t c = c.parents |> Lwt_list.map_p (of_hash t)

    let node t c = Nodes.of_hash t c.node

    let tree t c = node t c >>= Nodes.tree t

    let message c = c.message
  end

  (** {3 Operations on branches.} *)

  module Branches = struct
    let mem t = Backend.Branches.mem (branches_t t)

    let get_hash t b = Backend.Branches.find (branches_t t) b >|= Option.get

    let set_hash t b (CH h) = Backend.Branches.set (branches_t t) b h

    let get t b = get_hash t b >>= fun h -> Commits.of_hash t (CH h)

    let set t b c = Commits.to_hash c |> set_hash t b

    let remove t = Backend.Branches.remove (branches_t t)

    let list t = Backend.Branches.list (branches_t t)
  end

  (** {3 Creating database instances.} *)

  let create config = { backend = Backend.create config; config }

  (** {3 Opening workspaces.} *)

  let checkout t branch = Lwt.return { t; head = `Branch branch }

  let detach t commit = Lwt.return { t; head = `Head (ref (Some commit)) }

  (** {3 Operations on workspaces.} *)

  let database w = w.t

  let head w =
    match w.head with
    | `Branch b -> Branches.get w.t b
    | `Head r -> (
        match !r with
        | Some c -> Commits.of_hash w.t c
        | None -> invalid_arg "Head reference is not set." )

  let get_tree w p =
    let rec traverse p n =
      match (p, n) with
      | x :: xs, Node ns -> (
          match List.assoc_opt x ns with
          | Some h -> Nodes.of_hash w.t h >>= traverse xs
          | None -> invalid_arg "Invalid path (no such child)." )
      | [], n -> Nodes.tree w.t n
      | _ -> invalid_arg "Invalid path (reached blob)."
    in

    let%lwt c = head w in
    let%lwt n = Commits.node w.t c in
    traverse p n

  let get w p =
    get_tree w p >|= function
    | `Blob b -> b
    | _ -> invalid_arg "Path doesn't point to a blob."

  (** [update_head w h] sets the head of the current workspace to [h]. *)
  let update_head w h =
    match w.head with
    | `Branch b -> Branches.set_hash w.t b h
    | `Head r ->
        r := Some h;
        Lwt.return_unit

  (** [store_tree t tree] recursively stores [tree] in the node store of [t].
      Returns the hash of the root node of [tree]. *)
  let rec store_tree t tree =
    ( match tree with
    | `Blob b -> Backend.Blobs.set (blobs_t t) b >|= fun bh -> Blob (BH bh)
    | `Node ns -> map_snd (store_tree t) ns >|= fun nhs -> Node nhs )
    >>= fun n ->
    Backend.Nodes.set (nodes_t t) n >|= fun nh -> NH nh

  (** [merge_trees a b] recursively merges [a] and [b]. In case of a conflict,
      the subtree from [b] is always used. *)
  let rec merge_trees a b =
    match (a, b) with
    | `Node ca, `Node cb -> `Node (merge_children ca cb)
    | _ -> b

  and merge_children ca = function
    | (p, b) :: bs -> (
        match List.assoc_opt p ca with
        | Some a -> (p, merge_trees a b) :: merge_children ca bs
        | None -> (p, b) :: merge_children ca bs )
    | [] -> ca

  (** [update_node w p n] traverses the subtree starting at [n] until either:
      (1) Reaching the node [n'] at path [p]. (2) Reaching a dead end, with the
      remaining path [p'].

      On (1), [~on_found n'] is called and should return the hash of the updated
      version of [n']. On (2), [~on_dead_end p'] is called and should return the
      hash of the node to add at [p - p']. In any case, the update is propagated
      recursively to the parents, and the hash of the updated version of [n] is
      returned. *)
  let rec update_node ~on_found ~on_dead_end w p n =
    let rec aux = function
      | step :: steps, Node children ->
          (* Recursively update the children. *)
          ( match List.assoc_opt step children with
          | Some hash -> (
              Nodes.of_hash w.t hash >>= fun child ->
              aux (steps, child) >|= function
              | Some child' -> (step, child') :: List.remove_assoc step children
              | None -> List.remove_assoc step children )
          | None -> (
              aux (steps, Node []) >|= function
              | Some child' -> (step, child') :: children
              | None -> children ) )
          >>= fun children' ->
          (* Store the resulting node. *)
          Backend.Nodes.set (nodes_t w.t) (Node children') >|= fun nh ->
          (* Return its hash. *)
          Some (NH nh)
      | _ :: _, Blob _ -> on_dead_end p
      | [], _ -> on_found n
    in

    aux (p, n)

  let set_tree ?parents ?(message = "") ?(mode = `Merge) w p tree =
    let%lwt head_commit = head w in
    let%lwt head_node = Commits.node w.t head_commit in

    let on_found n =
      ( match mode with
      | `Set -> store_tree w.t tree
      | `Merge ->
          Nodes.tree w.t n >>= fun old -> store_tree w.t (merge_trees old tree)
      )
      >|= fun c -> Some c
    in

    let on_dead_end p =
      store_tree w.t (List.fold_right (fun s c -> `Node [ (s, c) ]) p tree)
      >|= fun c -> Some c
    in

    (* Store the updated nodes and the resulting commit. *)
    let%lwt node' = update_node ~on_found ~on_dead_end w p head_node in
    let node' = Option.get node' in
    let parents =
      match parents with Some p -> p | None -> head_commit.parents
    in
    let commit' = { parents; node = node'; message } in
    let%lwt ch' = Backend.Commits.set (commits_t w.t) commit' in
    let%lwt () = update_head w (CH ch') in
    Lwt.return commit'

  let set ?parents ?(message = "") w p blob =
    set_tree ?parents ~message w p (`Blob blob)

  let remove ?parents ?(message = "") w p =
    let%lwt head_commit = head w in
    let%lwt head_node = Commits.node w.t head_commit in

    let on_found _ = Lwt.return_none in
    let on_dead_end _ = Lwt.return_none in

    (* Store the updated nodes and the resulting commit. *)
    let%lwt node' = update_node ~on_found ~on_dead_end w p head_node in
    let node' = Option.get node' in
    let parents =
      match parents with Some p -> p | None -> head_commit.parents
    in
    let commit' = { parents; node = node'; message } in
    let%lwt ch' = Backend.Commits.set (commits_t w.t) commit' in
    let%lwt () = update_head w (CH ch') in
    Lwt.return commit'

  (** {3 Operations on the object graph.} *)

  module Graph = struct
    let iter ?depth ~min ~max ~node ~edge ~skip ~rev t =
      failwith "Not implemented."

    let export ?depth ?(full = false) ~date t buffer =
      failwith "Not implemented."

    let trace t = failwith "Not implemented."

    let cleanup ?(entry = `Head) t = failwith "Not implemented."
  end
end
