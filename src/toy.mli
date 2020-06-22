open S
module S = S

(** {1 Database creation.} *)

(** [Make (Backend) (Branch) (Step) (Blob)] returns an Irmin database module
    which stores values of hashable types [Blob] using the given [Backend]. The
    paths under which values are stored will have type [Step], and the branch
    handles will have type [Branch]. *)
module Make : functor
  (Hash : HASH)
  (Backend : BACKEND)
  (Branch : TYPE)
  (Step : TYPE)
  (Blob : TYPE)
  ->
  DATABASE
    with type branch = Branch.t
     and type step = Step.t
     and type blob = Blob.t
     and type config = Backend.config

(** [Basic (Backend)] returns an Irmin database module which stores strings
    under string paths, using the given [Backend]. *)
module Basic : functor (Hash : HASH) (Backend : BACKEND) ->
  DATABASE
    with type branch = string
     and type step = string
     and type blob = string
     and type config = Backend.config

(** {1 Available hash functions.} *)

module Hash = Hash

module Hashable = Type.Hashable
(** [Hashable (Hash) (Type)] turns a runtime type [Type] into a hashable type
    using the provided hash function. This will produce hashes computed with
    [hash x = Hash.to_hex (Hash.hash (Type.serialize x))]. *)

(** {1 Runtime types.} *)

module Types = Type.Types
(** Available runtime types. *)

val ( ~: ) : 'a Irmin.Type.t -> (module S.TYPE with type t = 'a)
(** [~:t] turns an Irmin runtime type [t] into a TYPE module, which can then be
    used to create a database or a Hashable. *)

(** {1 Available backend implementations.} *)

module Backend : sig
  module Memory : BACKEND with type config = unit
  (** In-memory backend using hashtables. *)

  module Filesystem : BACKEND with type config = string
  (** On-disk backend using one file per Irmin object. *)

  (** On-disk backends using large block files. *)
  module Block : sig
    module Copy : BACKEND with type config = string
    (** On-disk block backend with copy-on-filter behavior. *)

    module Lazy : BACKEND with type config = string
    (** On-disk block backend with lazy-filter behavior. *)
  end
end
