open S
module S = S
module Hash = Hash
module Hashable = Type.Hashable
module Type = Type.Types

module Backend = struct
  module Memory = Memory.Make
end

module Make = Database.Make
module Basic (Hash : HASH) (Backend : BACKEND) =
  Make (Hash) (Backend) (Type.String) (Type.String) (Type.String)
