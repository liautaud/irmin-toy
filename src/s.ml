(** Signatures of all the modules in the library. *)

(** {1 High-level interface.} *)

module type DATABASE = sig
  (** {2 Irmin database.} *)

  type t
  (** The type of databases.

      Similarly to a Git repository, an Irmin database keeps track of changes to
      a working tree over time. Databases are generally not used directly:
      instead, one needs to first obtain a workspace by either checking out a
      branch using [checkout], or detaching to a commit using [detach]. *)

  type workspace
  (** The type of workspaces.

      An Irmin workspace is a high-level view of the working tree of an Irmin
      database from the perspective of the current head, which can either point
      to a named branch or a single detached commit. *)

  type config = unit
  (** The type of configuration options used when creating a database. *)

  (* FIXME *)

  type branch
  (** The type of database branches. *)

  type commit
  (** The type of database commits. *)

  type node
  (** The type of database nodes. *)

  type blob
  (** The type of database blobs. *)

  type step
  (** The type of steps of the paths to nodes. *)

  type path = step list
  (** The type of paths to nodes in the database. *)

  type tree = [ `Blob of blob | `Node of (step * tree) list ]
  (** The type of trees in the database. *)

  type commit_hash
  (** The type of hashes to commits. *)

  type node_hash
  (** The type of hashes to nodes. *)

  type blob_hash
  (** The type of hashes to blobs. *)

  (** {3 Creating database instances.} *)

  val create : config -> t
  (** Creates a new database instance with the given configuration, which will
      be passed along to the backend. *)

  val initialize : master:branch -> t -> unit Lwt.t
  (** Initializes an empty database by creating an initial commit associated
      with an empty node, and a branch named [master] which points to that
      commit. *)

  (** {3 Opening workspaces.} *)

  val checkout : t -> branch -> workspace Lwt.t
  (** Returns a new workspace pointing to a given branch. This operation can be
      assumed to be cheap. *)

  val detach : t -> commit_hash -> workspace Lwt.t
  (** Returns a new workspace pointing to a given commit. This operation can be
      assumed to be cheap. *)

  (** {3 Operations on workspaces.} *)

  val database : workspace -> t
  (** [database w] is the database containing [w]. *)

  val head : workspace -> commit Lwt.t
  (** [head w] is the commit to which the head of [w] is currently pointing.
      Raises [Invalid_argument] if [w] doesn't point to a valid commit. *)

  val mem : workspace -> path -> bool Lwt.t
  (** [mem w path] returns whether [path] exists. *)

  val get : workspace -> path -> blob Lwt.t
  (** [get w path] returns the blob stored at [path]. Raises [Invalid_argument]
      if the path doesn't point to a blob. *)

  val get_tree : workspace -> path -> tree Lwt.t
  (** [get_tree w path] returns the whole subtree stored at [path]. Raises
      [Invalid_argument] if the path doesn't point to a valid subtree. *)

  val set :
    ?parents:commit list ->
    ?message:string ->
    workspace ->
    path ->
    blob ->
    commit Lwt.t
  (** [set w path blob] sets [path] to [blob]. This creates and returns a new
      commit with the given [~parents] and [~message]. *)

  val set_tree :
    ?parents:commit list ->
    ?message:string ->
    ?mode:[ `Merge | `Override ] ->
    workspace ->
    path ->
    tree ->
    commit Lwt.t
  (** [set_tree w path tree] sets the whole subtree at [path] to [tree]. This
      creates and returns a new commit with the given [~parents] and [~message].
      If [path] already contains a subtree and [~mode] is [`Merge], the stored
      subtree will be the combination of the previous subtree and [tree]. *)

  val remove :
    ?parents:commit list -> ?message:string -> workspace -> path -> commit Lwt.t
  (** [remove w path] removes the whole subtree at [path]. This creates and
      returns a new commit with the given [~parents] and [~message]. *)

  (** {3 Operations on branches.}

      Similarly to Git, branches in Irmin are used to gives user-defined names
      to commits. Every Irmin store starts with a "master" branch which
      initially points to the first commit. *)

  module Branches : sig
    val mem : t -> branch -> bool Lwt.t
    (** [mem t b] is true iff the branch [b] is present in [t]. *)

    val get : t -> branch -> commit Lwt.t
    (** [get t b] returns the commit to which the branch [b] points.

        Raise [Invalid_argument] if [b] is not present in [t]. *)

    val set : t -> branch -> commit -> unit Lwt.t
    (** [set t b c] binds the branch [b] to [c] in [t]. *)

    val remove : t -> branch -> unit Lwt.t
    (** [remove t b] removes the branch [b] from [t]. *)

    val list : t -> branch list Lwt.t
    (** [list t] is the list of branches present in [t]. *)
  end

  (** {3 Operations on commits.} *)
  module Commits : sig
    val to_hash : commit -> commit_hash
    (** [to_hash c] returns the type-safe hash of [c]. *)

    val of_hash : t -> commit_hash -> commit Lwt.t
    (** [of_hash t hash] returns the commit with the given hash in [t].

        Raises [Invalid_argument] if no such commit is stored in [t]. *)

    val parents : t -> commit -> commit list Lwt.t
    (** [parents t c] returns the list of parent commits of [c] in [t]. *)

    val node : t -> commit -> node Lwt.t
    (** [node t c] returns the tree node attached to [c]. *)

    val tree : t -> commit -> tree Lwt.t
    (** [tree t c] returns the entire tree attached to [c]. *)

    val message : commit -> string
    (** [message c] returns the commit message of [c]. *)
  end

  (** {3 Operations on nodes.} *)
  module Nodes : sig
    val to_hash : node -> node_hash
    (** [to_hash n] returns the type-safe hash of [n]. *)

    val of_hash : t -> node_hash -> node Lwt.t
    (** [of_hash t hash] returns the node with the given hash in [t].

        Raises [Invalid_argument] if no such node is stored in [t]. *)

    val children :
      t -> node -> (step * [ `Blob of blob | `Node of node ]) list Lwt.t
    (** [children t node] returns the list of nodes or blobs that are direct
        children of [node], along with their path relative to [node].

        Raises [Invalid_argument] if [node] is a blob. *)

    val tree : t -> node -> tree Lwt.t
    (** [tree t node] returns the entire subtree rooted at [node]. *)
  end

  (** {3 Operations on blobs.} *)
  module Blobs : sig
    val to_hash : blob -> blob_hash
    (** [to_hash b] returns the type-safe hash of [b]. *)

    val of_hash : t -> blob_hash -> blob Lwt.t
    (** [of_hash t hash] returns the blob with the given hash in [t].

        Raises [Invalid_argument] if no such blob is stored in [t]. *)
  end

  (** {3 Operations on the object graph.}

      The object graph is a directed graph which is implicitely formed by the
      the commits, nodes and blobs of an Irmin database. *)

  module Graph : sig
    type label = step option

    type vertex =
      [ `Branch of branch
      | `Commit of commit_hash
      | `Node of node_hash
      | `Blob of blob_hash ]

    val iter :
      ?depth:int ->
      ?full:bool ->
      ?rev:bool ->
      min:vertex list ->
      max:vertex list ->
      vertex:(vertex -> unit Lwt.t) ->
      edge:(label -> vertex -> vertex -> unit Lwt.t) ->
      skip:(vertex -> bool Lwt.t) ->
      t ->
      unit Lwt.t
    (** [iter ~depth ~full ~rev ~min ~max ~vertex ~edge ~skip t] iterates in
        over the object graph of database [t] starting with the [max] vertices
        and bounded by the [min] vertices and by [depth].

        It applies three functions while traversing the graph: [vertex] on the
        vertices; [edge label v predecessor_of_v] on the directed edges and
        [skip n] to not include a vertex [n], its predecessors and the outgoing
        edges of [n].

        If [full] is true (default is false) the full graph, including the
        commits, nodes and blobs, is traversed, otherwise it is the commit
        history graph only.

        If [rev] is true (default is true) then the graph is traversed in the
        reverse topological order: [vertex n] is applied only after it was
        applied on all its predecessors; [edge n p] is applied after [vertex n].
        Note that [edge n p] is applied even if [p] is skipped. *)

    val export :
      ?depth:int ->
      ?full:bool ->
      min:vertex list ->
      max:vertex list ->
      name:string ->
      t ->
      Buffer.t ->
      unit Lwt.t
    (** [export t ?depth ?full ~min ~max ~name t buf] exports the object graph
        of database [t] to the buffer [buf] using the Graphviz Dot format,
        starting with the [max] vertices and bounded by the [min] vertices and
        by [depth].

        If [full] is set (default is not) the full graph, including the commits,
        nodes and blobs, is exported, otherwise it is the commit history graph
        only. *)

    val cleanup : ?entry:[ `Branches | `List of commit list ] -> t -> unit Lwt.t
    (** [cleanup ~entry t] runs the garbage collector on the object graph of
        database [t]. All the commits, nodes and blobs which are not reachable
        from [entry] will be deleted from their back-end stores. *)
  end
end

(** {1 Runtime types.} *)

module type TYPE = sig
  (** {2 Runtime types.} *)
  type t
  (** The target type. *)

  val t : t Irmin.Type.t
  (** The runtime representation of the target type. *)
end

module type COMPARABLE = sig
  (** {2 Comparable types.}

      Comparable types are types which support ordering and equality. The
      easiest way to construct such a type to use [Type.Comparable_T]. *)

  include TYPE

  val equal : t -> t -> bool
  (** [equal a b] returns whether [a] and [b] are equal. *)

  val compare : t -> t -> int
  (** [compare a b] returns [-1] if a < b, [1] if a > b, and [0] otherwise. *)
end

module type SERIALIZABLE = sig
  (** {2 Serializable types.}

      Serializable types are comparable types which can be serialized into and
      deserialized from a binary representation at runtime. The easiest way to
      construct such a type to use the [Irmin.Type] library to define a runtime
      representation of the type, and use [ty_to_serializable]. *)

  include COMPARABLE

  val print : t -> string
  (** [print v] return the human-readable encoding of [v]. *)

  val serialize : t -> string
  (** [serialize v] return the binary encoding of [v]. *)

  val unserialize : string -> (t, [ `Msg of string ]) Stdlib.result
  (** [unserialize s] decodes the binary encoding [s], and returns either the
      decoded value or an error message. *)
end

module type HASHABLE = sig
  (** {2 Hashable types.}

      Hashable types are serializable types which can also be hashed at runtime.
      The easiest way to construct such a type is to use the [Irmin.Type]
      library to define a runtime representation of the type, and use
      [ty_to_hashable]. *)

  include SERIALIZABLE

  module Hash : SERIALIZABLE

  val hash : t -> Hash.t
  (** [hash v] returns the hash of [v]. *)
end

module type HASH = sig
  (** {2 Hash functions.} *)

  type t
  (** The type of digests produced by the hash function. *)

  val hash : string -> t
  (** [hash s] is a deterministic hash computed from the string [s]. *)

  val of_hex : string -> (t, [ `Msg of string ]) result
  (** [of_hex h] returns the digest represented by hex-string [h]. *)

  val to_hex : t -> string
  (** [to_hex h] returns the hexadecimal representation of [h]. *)
end

(** {1 Low-level storage and backends.} *)

module type IMMUTABLE_STORE = sig
  (** {2 Immutable backend stores.}

      Immutable stores provide a way to read and save values of some type using
      keys of another type. Depending on the backend, they might be implemented
      using in-memory hashtables, the underlying filesystem, or even at the
      block-device level.

      Immutable stores are not used directly by Irmin. Instead, Irmin uses
      content-addressable stores, which are created from immutable stores. *)

  type t
  (** The type of immutable backend stores. *)

  type key
  (** The type of stored keys. *)

  type value
  (** The type of stored values. *)

  val mem : t -> key -> bool Lwt.t
  (** [mem t k] is true iff [k] is present in [t]. *)

  val find : t -> key -> value option Lwt.t
  (** [find t k] is [Some v] if [k] is associated to [v] in [t] and [None] is
      [k] is not present in [t]. *)

  val set : t -> key -> value -> unit Lwt.t
  (** [set t k v] writes [v] to the store under key [k]. Does nothing if key [k]
      is already in the store. *)

  val filter : t -> (key -> bool) -> unit Lwt.t
  (** [filter t p] informs the immutable store that it can remove all values
      whose key [k] does not satisfy the predicate [p]. *)
end

module type CONTENT_ADDRESSABLE_STORE = sig
  (** {2 Content-addressable backend stores.}

      Content-addressable stores are a specialization of immutable stores which
      use the hash of the value as a key. This ensures that duplicate values are
      only stored once in the backend.

      Similarly to Git, Irmin uses content-addressable stores to save all its
      internal objects (commits, trees and blobs). Every type of object is saved
      to a different content-addressable store (respectively the commit, tree
      and blob stores), which are provided by the [BACKEND]. *)

  type t
  (** The type of content-addressable backend stores. *)

  type key
  (** The type of hashes generated by the store. *)

  type value
  (** The type of stored values. *)

  val mem : t -> key -> bool Lwt.t
  (** [mem t k] is true iff [k] is present in [t]. *)

  val find : t -> key -> value option Lwt.t
  (** [find t k] is [Some v] if [k] is associated to [v] in [t] and [None] is
      [k] is not present in [t]. *)

  val set : t -> value -> key Lwt.t
  (** [set t v] writes [v] to the store under a deterministically generated key,
      and returns that key. Does nothing if value [v] is already in the store. *)

  val filter : t -> (key -> bool) -> unit Lwt.t
  (** [filter t p] informs the content-addressable store that it can remove all
      values whose key [k] does not satisfy the predicate [p]. *)
end

module type ATOMIC_WRITE_STORE = sig
  (** {2 Atomically writable backend stores.}

      Atomically writable storaes provide a way to read, update and remove
      values of some type using keys of another type, plus a [test_and_set]
      operation which provides atomicity when setting values from multiple
      concurrent Lwt threads. *)

  type t
  (** The type of atomically writable backend stores. *)

  type key
  (** The type of stored keys. *)

  type value
  (** The type of stored values. *)

  val mem : t -> key -> bool Lwt.t
  (** [mem t k] is true iff [k] is present in [t]. *)

  val find : t -> key -> value option Lwt.t
  (** [find t k] is [Some v] if [k] is associated to [v] in [t] and [None] is
      [k] is not present in [t]. *)

  val set : t -> key -> value -> unit Lwt.t
  (** [set t k v] replaces the contents of [k] by [v] in [t]. If [k] is not
      already defined in [t], create a fresh binding. *)

  val test_and_set :
    t -> key -> test:value option -> set:value option -> bool Lwt.t
  (** [test_and_set t key ~test ~set] sets [key] to [set] only if the current
      value of [key] is [test] and in that case returns [true]. If the current
      value of [key] is different, it returns [false]. [None] means that the
      value does not have to exist or is removed.

      {b Note:} The operation is guaranteed to be atomic. *)

  val remove : t -> key -> unit Lwt.t
  (** [remove t k] removes the key [k] in [t]. *)

  val list : t -> key list Lwt.t
  (** [list t] returns the list of keys in [t]. *)
end

module type BACKEND = functor
  (Branch : HASHABLE)
  (Commit : HASHABLE)
  (Node : HASHABLE)
  (Blob : HASHABLE)
  -> sig
  (** {2 Backends.}

      Backends provide the underlying storage for Irmin databases. Specifically,
      they provide stores for the branches, commits, nodes and blobs which make
      up the object graph of the database. *)

  type t
  (** The type of backends. *)

  type config = unit
  (** The type of configuration options used when creating backends. *)

  (* FIXME *)

  val create : config -> t
  (** Creates a new backend for a given configuration. *)

  (** The atomically writable store used to store branches. *)
  module Branches :
    ATOMIC_WRITE_STORE with type key = Branch.t and type value = Commit.Hash.t

  (** The content-addressable store used to store commits. *)
  module Commits :
    CONTENT_ADDRESSABLE_STORE
      with type key = Commit.Hash.t
       and type value = Commit.t

  (** The content-addressable store used to store tree nodes. *)
  module Nodes :
    CONTENT_ADDRESSABLE_STORE
      with type key = Node.Hash.t
       and type value = Node.t

  (** The content-addressable store used to store blobs. *)
  module Blobs :
    CONTENT_ADDRESSABLE_STORE
      with type key = Blob.Hash.t
       and type value = Blob.t

  val branches_t : t -> Branches.t
  (** Returns a handle to the branch store. *)

  val commits_t : t -> Commits.t
  (** Returns a handle to the commit store. *)

  val nodes_t : t -> Nodes.t
  (** Returns a handle to the node store. *)

  val blobs_t : t -> Blobs.t
  (** Returns a handle to the blob store. *)
end
