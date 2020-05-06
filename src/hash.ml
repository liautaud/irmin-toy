module Make (H : Digestif.S) = struct
  type t = H.t

  let of_hex s =
    match H.consistent_of_hex s with
    | x -> Ok x
    | exception Invalid_argument e -> Error (`Msg e)

  let to_hex = H.to_hex

  let hash s = H.digest_string s
end

module SHA1 = Make (Digestif.SHA1)
module SHA256 = Make (Digestif.SHA256)
module SHA512 = Make (Digestif.SHA512)
