module type TYPE = sig
  type t

  val t : t Irmin.Type.t
end

module Comparable_T (T : TYPE) : S.COMPARABLE with type t = T.t = struct
  type t = T.t

  let t = T.t

  let equal = Irmin.Type.equal T.t

  let compare = Irmin.Type.compare T.t
end

module Serializable (T : S.COMPARABLE) = struct
  include T

  let serialize = Irmin.Type.to_bin_string T.t

  let unserialize = Irmin.Type.of_bin_string T.t
end

module Serializable_T (T : TYPE) = Serializable (Comparable_T (T))

module String = Serializable_T (struct
  type t = string

  let t = Irmin.Type.string
end)

module Hashable (H : S.HASH) (T : S.SERIALIZABLE) = struct
  include T

  let hash x = serialize x |> H.hash |> H.to_hex

  module Hash = String
end

module Hashable_T (H : S.HASH) (T : TYPE) = Hashable (H) (Serializable_T (T))
