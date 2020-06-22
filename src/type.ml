let ( ~: ) : type t'. t' Irmin.Type.t -> (module S.TYPE with type t = t') =
 fun t' ->
  ( module struct
    type t = t'

    let t = t'
  end )

module Comparable (T : S.TYPE) : S.COMPARABLE with type t = T.t = struct
  type t = T.t

  let equal = Irmin.Type.equal T.t

  let compare = Irmin.Type.compare T.t
end

module Serializable (T : S.TYPE) = struct
  include Comparable (T)

  let serialize = Irmin.Type.to_bin_string T.t

  let unserialize = Irmin.Type.of_bin_string T.t
end

module Hashable (H : S.HASH) (T : S.TYPE) = struct
  include Serializable (T)
  module Digest = H.Digest

  let hash x = serialize x |> H.hash
end

module Types = struct
  module Unit = (val ~:Irmin.Type.unit)

  module Int = (val ~:Irmin.Type.int)

  module Int32 = (val ~:Irmin.Type.int32)

  module Int64 = (val ~:Irmin.Type.int64)

  module Bool = (val ~:Irmin.Type.bool)

  module Float = (val ~:Irmin.Type.float)

  module String = (val ~:Irmin.Type.string)

  module Bytes = (val ~:Irmin.Type.bytes)
end
