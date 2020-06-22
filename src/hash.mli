(** Hash functions provided by Digestif. *)

(** [Make (H)] creates a hash function from a Digestif hash function. *)
module Make (H : Digestif.S) : S.HASH with type Digest.t = H.t

module SHA1 : S.HASH
(** SHA1-based hash function. *)

module SHA256 : S.HASH
(** SHA1-based hash function. *)

module SHA512 : S.HASH
(** SHA1-based hash function. *)
