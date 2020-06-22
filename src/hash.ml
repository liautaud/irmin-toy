module Make (H : Digestif.S) = struct
  let hash s = H.digest_string s

  module Digest = struct
    type t = H.t

    let size = H.digest_size

    let equal = H.equal

    let compare = H.unsafe_compare

    let serialize = H.to_raw_string

    let unserialize b =
      H.of_raw_string_opt b
      |> Option.to_result ~none:(`Msg "Invalid hash digest.")

    let hex_size = size * 2

    let of_hex s =
      match H.consistent_of_hex s with
      | x -> Ok x
      | exception Invalid_argument e -> Error (`Msg e)

    let to_hex = H.to_hex
  end
end

module SHA1 = Make (Digestif.SHA1)
module SHA256 = Make (Digestif.SHA256)
module SHA512 = Make (Digestif.SHA512)
