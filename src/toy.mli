open S

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
  -> DATABASE

(** [Basic (Backend)] returns an Irmin database module which stores strings
    under string paths, using the given [Backend]. *)
module Basic : functor (Hash : HASH) (Backend : BACKEND) -> DATABASE

(** {1 Available runtime types.} *)

module Types : sig
  module String : SERIALIZABLE
end

(** {1 Available backend implementations.} *)

module Backends : sig
  module Memory : BACKEND
  (** In-memory backend using hashtables. *)
end
