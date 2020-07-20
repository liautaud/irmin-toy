open S
module S = S
module Hash = Hash
module Hashable = Type.Hashable
module Types = Type.Types

let ( ~: ) = Type.( ~: )

module Backend = struct
  module Memory = Memory.Backend
  module Filesystem = Filesystem.Backend

  module Block = struct
    module Copy = Block_copy.Backend
    module Lazy = Block_lazy.Backend
  end
end

let src = Database.src
module Make = Database.Make
module Basic (Hash : HASH) (Backend : BACKEND) =
  Make (Hash) (Backend) (Types.String) (Types.String) (Types.String)
