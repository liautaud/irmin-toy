let suite =
  [
    Database.suite;
    Hashes.suite;
    Tracing.suite;
    Backend.make (module Toy.Backend.Memory) "memory";
  ]

let () = Alcotest.run "irmin-toy" suite
