(** [Full (Backend) (Branch) (Step) (Blob)] returns an Irmin database module
    which stores values of hashable types [Blob] using the given [Backend]. The
    paths under which values are stored will have type [Step], and the branch
    handles will have type [Branch]. *)
module Full : functor
  (Hash : S.HASH)
  (Backend : S.BACKEND)
  (Branch : S.HASHABLE)
  (Step : S.HASHABLE)
  (Blob : S.HASHABLE)
  -> S.DATABASE

(** [Simple (Backend) (Blob)] returns an Irmin database module which stores
    values of hashable type [Blob] using the given [Backend]. *)
module Simple : functor
  (Hash : S.HASH)
  (Backend : S.BACKEND)
  (Blob : S.HASHABLE)
  -> S.DATABASE
