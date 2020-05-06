(** In-memory backend for irmin-toy.

    This backend stores all branches, commits, nodes and blobs into separate
    hashtables in memory. A new set of hashtables is created each time the
    backend is instanciated, so the same Backend module can be reused in
    multiple databases. *)

module Memory : S.BACKEND
