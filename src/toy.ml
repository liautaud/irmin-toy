open S
module Types = Type.Types
module Make = Database.Make
module Basic (Hash : HASH) (Backend : BACKEND) =
  Make (Hash) (Backend) (Types.String) (Types.String) (Types.String)

module Backends = struct
  module Memory = Memory.Make
end
