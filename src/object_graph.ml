(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *                    2020 Romain Liautaud <romain.liautaud@tarides.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix

let list_partition_map f t =
  let rec aux fst snd = function
    | [] -> (List.rev fst, List.rev snd)
    | h :: t -> (
        match f h with
        | `Fst x -> aux (x :: fst) snd t
        | `Snd x -> aux fst (x :: snd) t )
  in
  aux [] [] t

module Make (Label : S.TYPE) (Vertex : S.SERIALIZABLE) = struct
  type label = Label.t
  (** Labels of the edges in the graph. *)

  type vertex = Vertex.t
  (** Vertices in the graph. *)

  module X = struct
    type t = Vertex.t

    let equal = Vertex.equal

    let compare = Vertex.compare

    let hash x = Irmin.Type.short_hash Vertex.t x
  end

  module Table = Hashtbl.Make (X)

  let vertex_attributes = ref (fun _ -> [])

  let edge_attributes = ref (fun _ -> [])

  let graph_name = ref None

  module G = Graph.Imperative.Digraph.ConcreteBidirectional (X)

  module Dot = Graph.Graphviz.Dot (struct
    include G

    let edge_attributes k = !edge_attributes k

    let default_edge_attributes _ = []

    let vertex_name = Vertex.print

    let vertex_attributes k = !vertex_attributes k

    let default_vertex_attributes _ = []

    let get_subgraph _ = None

    let graph_attributes _ =
      match !graph_name with None -> [] | Some n -> [ `Label n ]
  end)

  let iter ?(depth = max_int) ~pred ~min ~max ~vertex ~edge ~skip ~rev () =
    let marks = Table.create 1024 in
    let mark key level = Table.add marks key level in
    let has_mark key = Table.mem marks key in
    let todo = Stack.create () in
    List.iter (fun k -> Stack.push (k, 0) todo) max;
    let treat key =
      vertex key >>= fun () ->
      if not (List.mem key min) then
        pred key >>= fun keys ->
        Lwt_list.iter_p (fun (label, k) -> edge label key k) keys
      else Lwt.return_unit
    in
    let rec pop key level =
      ignore (Stack.pop todo);
      mark key level;
      visit ()
    and visit () =
      match Stack.top todo with
      | exception Stack.Empty -> Lwt.return_unit
      | key, level -> (
          if level >= depth then pop key level
          else if has_mark key then (
            (if rev then treat key else Lwt.return_unit) >>= fun () ->
            ignore (Stack.pop todo);
            visit () )
          else
            skip key >>= function
            | true -> pop key level
            | false ->
                (if not rev then treat key else Lwt.return_unit) >>= fun () ->
                mark key level;
                if List.mem key min then visit ()
                else
                  pred key >>= fun keys ->
                  List.iter
                    (fun (_, k) ->
                      if not (has_mark k) then Stack.push (k, level + 1) todo)
                    keys;
                  visit () )
    in
    visit ()

  let output ppf vertex edges name =
    let g = G.create ~size:(List.length vertex) () in
    List.iter (fun (v, _) -> G.add_vertex g v) vertex;
    List.iter (fun (v1, _, v2) -> G.add_edge g v1 v2) edges;
    let eattrs (v1, v2) =
      try
        let l = List.filter (fun (x, _, y) -> x = v1 && y = v2) edges in
        let l = List.fold_left (fun acc (_, l, _) -> l @ acc) [] l in
        let labels, others =
          list_partition_map (function `Label l -> `Fst l | x -> `Snd x) l
        in
        match labels with
        | [] -> others
        | [ l ] -> `Label l :: others
        | _ -> `Label (String.concat "," labels) :: others
      with Not_found -> []
    in
    let vattrs v = try List.assoc v vertex with Not_found -> [] in
    vertex_attributes := vattrs;
    edge_attributes := eattrs;
    graph_name := Some name;
    Dot.fprint_graph ppf g
end
