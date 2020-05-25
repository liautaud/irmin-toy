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
  (Branch : SERIALIZABLE)
  (Step : SERIALIZABLE)
  (Blob : SERIALIZABLE)
  ->
  DATABASE
    with type branch = Branch.t
     and type step = Step.t
     and type blob = Blob.t

(** [Basic (Backend)] returns an Irmin database module which stores strings
    under string paths, using the given [Backend]. *)
module Basic : functor (Hash : HASH) (Backend : BACKEND) ->
  DATABASE
    with type branch = string
     and type step = string
     and type blob = string

(** {1 Available hash functions.} *)

module Hash = Hash

module Hashable = Type.Hashable
(** [Hashable (Hash) (Type)] turns a serializable runtime type [Type] into a
    hashable type using the provided hash function. This will produce hashes
    computed with [hash x = H.to_hex (H.hash (T.serialize x))]. *)

(** {1 Available runtime types.} *)

module Type = Type.Types

(** {1 Available backend implementations.} *)

module Backend : sig
  module Memory : BACKEND
  (** In-memory backend using hashtables. *)
end
