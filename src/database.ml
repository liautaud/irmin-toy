open Lwt.Infix
open Type
open Printf
open Astring

let map_snd f = Lwt_list.map_p (fun (a, b) -> f b >|= fun b -> (a, b))

module Make
    (Hash : S.HASH)
    (Backend : S.BACKEND)
    (Branch : S.SERIALIZABLE)
    (Step : S.SERIALIZABLE)
    (Blob : S.SERIALIZABLE) =
struct
  type branch = Branch.t

  let branch_t = Branch.t

  type step = Step.t

  let step_t = Step.t

  type blob = Blob.t

  type commit_hash = CH of string [@@unboxed] [@@deriving irmin]

  type node_hash = NH of string [@@unboxed] [@@deriving irmin]

  type blob_hash = BH of string [@@unboxed] [@@deriving irmin]

  type node = (step * [ `Node of node_hash | `Blob of blob_hash ]) list
  [@@deriving irmin]

  type path = step list

  type tree = [ `Blob of blob | `Node of (step * tree) list ]

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

  type t = {
    backend : Backend.t;
    config : config;
    (* Lock used to avoid writing new objects during garbage collection. *)
    write_lock : Lwt_mutex.t;
  }

  let branches_store t = Backend.branches_t t.backend

  let commits_store t = Backend.commits_t t.backend

  let nodes_store t = Backend.nodes_t t.backend

  let blobs_store t = Backend.blobs_t t.backend

  type workspace = {
    t : t;
    (* Pointer to the current branch or the current detached commit. *)
    head : [ `Branch of branch | `Head of commit_hash option ref ];
  }

  (** {3 Operations on blobs.} *)

  module Blobs = struct
    let set t b =
      Lwt_mutex.with_lock t.write_lock (fun () ->
          Backend.Blobs.set (blobs_store t) b)

    let to_hash b = BH (Blob.hash b)

    let of_hash t (BH h) = Backend.Blobs.find (blobs_store t) h >|= Option.get
  end

  (** {3 Operations on nodes.} *)

  module Nodes = struct
    let set t n =
      Lwt_mutex.with_lock t.write_lock (fun () ->
          Backend.Nodes.set (nodes_store t) n)

    let to_hash n = NH (Node.hash n)

    let of_hash t (NH h) = Backend.Nodes.find (nodes_store t) h >|= Option.get

    let children t n =
      n
      |> map_snd (function
           | `Blob b -> Blobs.of_hash t b >|= fun b -> `Blob b
           | `Node n -> of_hash t n >|= fun b -> `Node b)

    let tree t n =
      let rec aux n =
        n
        |> map_snd (function
             | `Blob b -> Blobs.of_hash t b >|= fun b -> `Blob b
             | `Node n -> of_hash t n >>= aux >|= fun b -> `Node b)
      in
      aux n >|= fun n -> `Node n
  end

  (** {3 Operations on commits.} *)

  module Commits = struct
    let set t c =
      Lwt_mutex.with_lock t.write_lock (fun () ->
          Backend.Commits.set (commits_store t) c)

    let to_hash c = CH (Commit.hash c)

    let of_hash t (CH h) =
      Backend.Commits.find (commits_store t) h >|= Option.get

    let parents t c = c.parents |> Lwt_list.map_p (of_hash t)

    let node t c = Nodes.of_hash t c.node

    let tree t c = node t c >>= Nodes.tree t

    let message c = c.message
  end

  (** {3 Operations on branches.} *)

  module Branches = struct
    let mem t = Backend.Branches.mem (branches_store t)

    let get_commit_hash t b =
      Backend.Branches.find (branches_store t) b >|= function
      | Some h -> CH h
      | _ -> invalid_arg "Branch not found."

    let set_commit_hash t b (CH h) = Backend.Branches.set (branches_store t) b h

    let get t b = get_commit_hash t b >>= fun h -> Commits.of_hash t h

    let set t b c = Commits.to_hash c |> set_commit_hash t b

    let remove t = Backend.Branches.remove (branches_store t)

    let list t = Backend.Branches.list (branches_store t)
  end

  (** {3 Creating database instances.} *)

  let create config =
    {
      backend = Backend.create config;
      config;
      write_lock = Lwt_mutex.create ();
    }

  let initialize ~master t =
    let message = "Initial commit." in
    let%lwt node_hash = Nodes.set t [] in
    let commit = { parents = []; node = NH node_hash; message } in
    let%lwt commit_hash = Commits.set t commit in
    Branches.set_commit_hash t master (CH commit_hash)

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
      match p with
      | [] -> invalid_arg "Invalid path (should not be empty)."
      | x :: xs -> (
          match (List.assoc_opt x n, xs) with
          | Some (`Node h), [] -> Nodes.of_hash w.t h >>= Nodes.tree w.t
          | Some (`Node h), _ -> Nodes.of_hash w.t h >>= traverse xs
          | Some (`Blob h), [] -> Blobs.of_hash w.t h >|= fun b -> `Blob b
          | Some (`Blob _), _ -> invalid_arg "Invalid path (reached blob)."
          | None, _ -> invalid_arg "Invalid path (no such child)." )
    in

    let%lwt c = head w in
    let%lwt n = Commits.node w.t c in
    traverse p n

  let mem w p =
    try%lwt
      let%lwt _ = get_tree w p in
      Lwt.return_true
    with Invalid_argument _ -> Lwt.return_false

  let get w p =
    get_tree w p >|= function
    | `Blob b -> b
    | _ -> invalid_arg "Path doesn't point to a blob."

  (** [update_head w h] sets the head of the current workspace to [h]. *)
  let update_head w h =
    match w.head with
    | `Branch b -> Branches.set_commit_hash w.t b h
    | `Head r ->
        r := Some h;
        Lwt.return_unit

  (** [store_tree t tree] recursively stores [tree] in the node store of [t].
      Returns the hash of the root node of [tree]. *)
  let rec store_tree t tree =
    match tree with
    | `Blob b -> Blobs.set t b >|= fun bh -> `Blob (BH bh)
    | `Node ns ->
        map_snd (store_tree t) ns >>= Nodes.set t >|= fun nh -> `Node (NH nh)

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
      (1) Reaching the node or blob [n'] at path [p]. (2) Reaching a dead end,
      with the remaining path [p'].

      On (1), [~on_found n'] is called and should return the hash of the updated
      version of [n']. On (2), [~on_dead_end p'] is called and should return the
      hash of the node to add at [p - p']. In any case, the update is propagated
      recursively to the parents, and the hash of the updated version of [n] is
      returned. *)
  let update_node ~on_found ~on_dead_end t p n =
    let fetch = function
      | `Node hash -> Nodes.of_hash t hash >|= fun n -> `Node n
      | `Blob hash -> Blobs.of_hash t hash >|= fun b -> `Blob b
    in
    let rec aux = function
      | step :: steps, `Node children ->
          (* Recursively update the children. *)
          ( match List.assoc_opt step children with
          | Some hash -> (
              let others = List.remove_assoc step children in
              fetch hash >>= fun child ->
              aux (steps, child) >|= function
              | Some child' -> (step, child') :: others
              | None -> others )
          | None -> (
              on_dead_end steps >|= function
              | Some child' -> (step, child') :: children
              | None -> children ) )
          >>= fun children' ->
          (* Store the resulting node. *)
          Nodes.set t children' >|= fun nh ->
          (* Return its hash. *)
          Some (`Node (NH nh))
      | step :: steps, `Blob _ -> on_dead_end (step :: steps)
      | [], n -> on_found n
    in

    aux (p, `Node n) >|= function
    | Some (`Node h') -> h'
    | _ -> invalid_arg "The path should not be empty."

  let set_tree ?parents ?(message = "") ?(mode = `Merge) w p tree =
    let%lwt head_commit = head w in
    let%lwt head_node = Commits.node w.t head_commit in

    (* When reaching the target node, store either [tree] or the merge between
       [tree] and the previous tree depending on [mode], and return its hash. *)
    let on_found n =
      ( match n with
      | `Blob _ -> store_tree w.t tree
      | `Node n -> (
          match mode with
          | `Override -> store_tree w.t tree
          | `Merge ->
              Nodes.tree w.t n >>= fun old ->
              store_tree w.t (merge_trees old tree) ) )
      >|= fun c -> Some c
    in

    (* When a suffix [x / y / ... / z] of the path [p] isn't reachable, create
       a tree with the shape [`Node [(x, `Node [(y, ... `Node [(z, tree)])])]],
       store it, and return its hash. *)
    let on_dead_end p =
      store_tree w.t (List.fold_right (fun s c -> `Node [ (s, c) ]) p tree)
      >|= fun c -> Some c
    in

    (* Store the updated nodes and the resulting commit. *)
    let%lwt n = update_node ~on_found ~on_dead_end w.t p head_node in
    let p =
      match parents with
      | Some p -> List.map Commits.to_hash p
      | None -> [ Commits.to_hash head_commit ]
    in
    let c = { parents = p; node = n; message } in
    let%lwt ch = Commits.set w.t c in
    let%lwt () = update_head w (CH ch) in
    Lwt.return c

  let set ?parents ?(message = "") w p blob =
    set_tree ?parents ~message w p (`Blob blob)

  let remove ?parents ?(message = "") w p =
    let%lwt head_commit = head w in
    let%lwt head_node = Commits.node w.t head_commit in

    (* Return None in both cases to request the deletion of the node. *)
    let on_found _ = Lwt.return_none in
    let on_dead_end _ = Lwt.return_none in

    (* Store the updated nodes and the resulting commit. *)
    let%lwt n = update_node ~on_found ~on_dead_end w.t p head_node in
    let p =
      match parents with
      | Some p -> List.map Commits.to_hash p
      | None -> head_commit.parents
    in
    let c = { parents = p; node = n; message } in
    let%lwt ch = Commits.set w.t c in
    let%lwt () = update_head w (CH ch) in
    Lwt.return c

  (** {3 Operations on the object graph.} *)

  module Graph = struct
    type label = step option [@@deriving irmin]

    type vertex =
      [ `Branch of branch
      | `Commit of commit_hash
      | `Node of node_hash
      | `Blob of blob_hash ]
    [@@deriving irmin]

    module Label_T = struct
      type t = label

      let t = label_t
    end

    module Vertex_T = struct
      include Serializable_T (struct
        type t = vertex

        let t = vertex_t
      end)

      let print = function
        | `Branch b -> Printf.sprintf "b:%s" (Branch.print b)
        | `Commit (CH h) -> Printf.sprintf "c:%s" h
        | `Node (NH h) -> Printf.sprintf "n:%s" h
        | `Blob (BH h) -> Printf.sprintf "b:%s" h
    end

    module OGraph = Object_graph.Make (Label_T) (Vertex_T)

    let iter ?depth ?(full = false) ?(rev = true) ~min ~max ~vertex ~edge ~skip
        t =
      let to_commit x : vertex = `Commit x in
      let to_node x : vertex = `Node x in
      let to_blob x : vertex = `Blob x in

      (* Returns the predecessors of the current vertex. *)
      let pred : vertex -> (label * vertex) list Lwt.t = function
        | `Branch b ->
            Branches.get_commit_hash t b >|= fun h -> [ (None, to_commit h) ]
        | `Commit k ->
            Commits.of_hash t k >|= fun c ->
            let next_commits =
              c.parents |> List.map (fun c -> (None, to_commit c))
            in
            let next_node = (None, c.node |> to_node) in
            if full then next_node :: next_commits else next_commits
        | `Node k ->
            Nodes.of_hash t k
            >|= List.map (function
                  | step, `Node n -> (Some step, to_node n)
                  | step, `Blob b -> (Some step, to_blob b))
        | `Blob _ -> Lwt.return []
      in

      (* Use the generic graph traversal algorithm. *)
      OGraph.iter ?depth ~pred ~min ~max ~vertex ~edge ~skip ~rev ()

    let export ?depth ?full ~min ~max ~name t buf =
      (* Prepare stores for the Graphviz vertices and edges. *)
      let vertices = Hashtbl.create 102 in
      let add_vertex v l = Hashtbl.add vertices v l in
      let mem_vertex v = Hashtbl.mem vertices v in
      let edges = ref [] in
      let add_edge v1 v2 l =
        if mem_vertex v1 && mem_vertex v2 then edges := (v2, l, v1) :: !edges
      in

      (* Define formatting of Graphviz vertices and edges. *)
      let trunc h =
        if String.length h <= 8 then h else String.with_range h ~len:8
      in

      let label_of_branch k = Lwt.return (`Label (Branch.print k)) in
      let label_of_commit (CH h) =
        Commits.of_hash t (CH h) >|= fun c ->
        `Label (sprintf "%s (%s)" (trunc h) c.message)
      in
      let label_of_node (NH h) = Lwt.return (`Label (trunc h)) in
      let label_of_blob (BH h) =
        Blobs.of_hash t (BH h) >|= fun b ->
        `Label (sprintf "%s (%s)" (trunc h) (Blob.print b))
      in
      let label_of_step s = `Label (Step.print s) in

      (* Traverse the object graph. *)
      let vertex v =
        match v with
        | `Branch b ->
            label_of_branch b >|= fun label ->
            add_vertex (`Branch b) [ `Shape `Plaintext; `Style `Filled; label ]
        | `Commit k ->
            label_of_commit k >|= fun label ->
            add_vertex (`Commit k) [ `Shape `Box; `Style `Bold; label ]
        | `Node k ->
            label_of_node k >|= fun label ->
            add_vertex (`Node k) [ `Shape `Box; `Style `Dotted; label ]
        | `Blob k ->
            label_of_blob k >|= fun label ->
            add_vertex (`Blob k) [ `Shape `Box; `Style `Dotted; label ]
      in
      let edge label v pred =
        ( match (v, pred, label) with
        | `Node _, `Node _, Some step ->
            add_edge pred v [ `Style `Solid; label_of_step step ]
        | `Node _, `Blob _, Some step ->
            add_edge pred v [ `Style `Dotted; label_of_step step ]
        | `Commit _, `Commit _, _ -> add_edge pred v [ `Style `Bold ]
        | `Commit _, `Node _, _ -> add_edge pred v [ `Style `Dashed ]
        | `Branch _, `Commit _, _ -> add_edge pred v [ `Style `Bold ]
        | _ -> () );
        Lwt.return_unit
      in
      let skip _ = Lwt.return false in
      iter ?depth ?full ~min ~max ~vertex ~edge ~skip t >|= fun () ->
      (* Use OCamlGraph to output the graph in Dot format. *)
      let vertices = Hashtbl.fold (fun k v acc -> (k, v) :: acc) vertices [] in
      let edges = !edges in

      let ppf = Format.formatter_of_buffer buf in
      OGraph.output ppf vertices edges name

    let trace ?(entry = `Branches) t =
      (* Find the entry-points for the graph traversal. *)
      ( match entry with
      | `Branches ->
          (* Lists the commits currently associated to the branches. *)
          Branches.list t >>= fun branches ->
          Lwt_list.map_p (Branches.get_commit_hash t) branches
      | `List commits ->
          (* Lets the user specify a custom list of commits as heads. *)
          commits |> List.map Commits.to_hash |> Lwt.return )
      >|= List.map (fun c -> `Commit c)
      >>= fun max ->
      (* Traverse the graph and mark the encountered objects. *)
      let table = OGraph.Table.create 1024 in
      let min = [] in
      let vertex n =
        OGraph.Table.add table n ();
        Lwt.return_unit
      in
      let edge _ _ _ = Lwt.return_unit in
      let skip _ = Lwt.return_false in
      iter ~min ~max ~vertex ~edge ~skip ~rev:false t >|= fun () -> table

    let cleanup ?entry t =
      (* Temporarily prevents writes to backend stores from happening so that
         objects don't accidentaly get ignored by the collector. Note that we
         don't block the creation and update of branches, so when using the
         entrypoint `Branches we only guarantee that the collection starts
         from the branches available when the collector is started. *)
      Lwt_mutex.with_lock t.write_lock (fun () ->
          trace ?entry t >>= fun table ->
          (* Filter the stores to keep only the encountered objects. *)
          Backend.Commits.filter (commits_store t) (fun k ->
              OGraph.Table.mem table (`Commit (CH k)))
          >>= fun () ->
          Backend.Nodes.filter (nodes_store t) (fun k ->
              OGraph.Table.mem table (`Node (NH k)))
          >>= fun () ->
          Backend.Blobs.filter (blobs_store t) (fun k ->
              OGraph.Table.mem table (`Blob (BH k))))
  end
end
