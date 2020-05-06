(** Filesystem backend for irmin-toy.

    This backend stores all branches, commits, nodes and blobs into separate
    files on separate folders of the host filesystem. The backend is configured
    by passing the path to the root folder used for storage. *)

module Filesystem : S.BACKEND
