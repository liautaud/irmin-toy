module Comparable_T (T : S.TYPE) : S.COMPARABLE with type t = T.t = struct
  type t = T.t

  let t = T.t

  let equal = Irmin.Type.equal T.t

  let compare = Irmin.Type.compare T.t
end

module Serializable (T : S.COMPARABLE) = struct
  include T

  let print = Irmin.Type.to_string T.t

  let serialize = Irmin.Type.to_bin_string T.t

  let unserialize = Irmin.Type.of_bin_string T.t
end

module Serializable_T (T : S.TYPE) = Serializable (Comparable_T (T))

module Types = struct
  module String = Serializable_T (struct
    type t = string

    let t = Irmin.Type.string
  end)
end

module Hashable (H : S.HASH) (T : S.SERIALIZABLE) = struct
  include T

  let hash x = serialize x |> H.hash |> H.to_hex

  module Hash = Types.String
end

module Hashable_T (H : S.HASH) (T : S.TYPE) = Hashable (H) (Serializable_T (T))
