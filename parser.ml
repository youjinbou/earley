
let fmt ppf f = Format.fprintf ppf f
let sfmt f = Format.(fprintf str_formatter) f
let efmt f = Format.eprintf f

module type TOKEN = sig
    type t
    type lexbuf
    val compare : t -> t -> int
    val lexeme : lexbuf -> t option
    val to_string : t -> string
end

module MultiMap = struct

  module type OrderedType = Map.OrderedType

  module type S = sig
      type key
      type value
      module Set : Set.S with type elt = value
             
      type t
      val empty : t
      val is_empty : t -> bool
      val mem : key -> t -> bool
      val add : key -> value -> t -> t
      val add_set : key -> Set.t -> t -> t
      val add_list : key -> value list -> t -> t
      val singleton : key -> value -> t
      val remove : key -> t -> t
      val merge  : t -> t -> t
      (*
      val compare : ( value -> value -> int) ->  t -> t -> int
      val equal : (value -> value -> bool) -> t -> t -> bool
       *)
      val iter_set : (key -> Set.t -> unit) -> t -> unit
      val iter : (key -> value -> unit) -> t -> unit
      val fold : (key -> value -> 'b -> 'b) -> t -> 'b -> 'b
      val for_all : (key -> value -> bool) -> t -> bool
      val exists : (key -> value -> bool) -> t -> bool
      val filter : (key -> value -> bool) -> t -> t
      val partition : (key -> value -> bool) -> t -> t * t
      val bindings : t -> (key * value) list
      (*
      val cardinal : t -> int
      val min_binding : t -> key * value
      val max_binding : t -> key * value
      val choose : t -> key * value
      val split : key -> t -> t * value option * t
       *)
      val find : key -> t -> Set.t
      val map : (value -> value) -> t -> t
      val mapi : (key -> value -> value) -> t -> t
    end

  (* builds a multimap out of Map and Set:
   * keys are mapped to many values without duplicate
   * (ie. for all keys, for all values, the pair (key,value) is unique)
   * adding a value already in the map will leave it unchanged *)
  module Make(OK : OrderedType) (OV : OrderedType) : S with type key = OK.t
                                                        and type value = OV.t = struct

    type key = OK.t
    type value = OV.t

    module M = Map.Make(OK)
    module S = Set.Make(OV)
    module Set = S

    type t = S.t M.t

    let empty = M.empty
    let is_empty = M.is_empty

    let mem k m = M.mem k m

    let add k x m =
      try let xl = M.find k m in
          let xl' = S.add x xl in
          if xl' == xl then m else M.add k xl' m
      with Not_found -> M.add k (S.singleton x) m

    let add_set k x m =
      try let xl = M.find k m in
          let xl' = S.union xl x in
          if xl' == xl then m else M.add k xl' m
      with Not_found -> M.add k x m

    let add_list k x m =
      let xl =
        try M.find k m
        with Not_found -> S.empty in
      let xl' = List.fold_right S.add x xl in
      if xl' == xl then m else M.add k xl' m

    let singleton k x = M.singleton k (S.singleton x)

    let remove = M.remove

    let merge (ma : t) (mb : t) =
      let merge k a b =
        match a,b with
          Some la, Some lb -> Some (S.union la lb)
        | l, None
        | None, l -> l
      in M.merge merge ma mb

(*
    let compare = M.compare
    let equal = M.equal
 *)

    let iter_set f m = M.iter f m

    let iter f m =
      M.iter (fun k l -> S.iter (f k) l) m

    let fold f m x =
      M.fold (fun k s x -> S.fold (f k) s x) m x

    let for_all f m =
      M.for_all (fun k l -> S.for_all (f k) l) m

    let exists f m : bool =
      M.exists (fun k l -> S.exists (f k) l) m
               
    let filter f (m : t) : t =
      M.fold (fun k l n -> let l' = S.filter (f k) l in M.add k l' n) m M.empty
      
    let partition f (m : t) : t * t =
      M.fold (fun k l (n1,n2) -> let l1, l2 = S.partition (f k) l in M.add k l1 n1, M.add k l2 n2) m (M.empty,M.empty)

    let bindings m =
      List.flatten (M.bindings m |> List.map (fun (k,s) -> List.map (fun x -> k,x) @@ S.elements s))
(*      
    let min_binding : 'a t -> key * 'a
    let max_binding : 'a t -> key * 'a
    let choose : 'a t -> key * 'a
    let split : key -> 'a t -> 'a t * 'a option * 'a t
 *)
    let find k m = M.find k m

    let map f (m : t) : t =
      M.map (fun l -> S.fold (fun v s -> S.add (f v) s) l S.empty) m

    let mapi f (m : t) : t =
      M.mapi (fun k l ->  S.fold (fun v s -> S.add (f k v) s) l S.empty) m

  end

end

module Make(T : TOKEN) = struct

  (* token id *)
  type id = T.t

  (* grammar token *)
  module Symbol = struct
    type t =  
        Terminal of id
      | NonTerminal of id

    let pp ppf x =
      let k, x =
        match x with Terminal x -> "t", x | NonTerminal x -> "nt", x in
      fmt ppf "%s %s" k (T.to_string x)

    let compare a b =
      let ret x = x
                    (*
        efmt "comparing %a %a -> %d\n%!" pp a pp b x; x
                     *)
      in
      match a, b with
        Terminal x, Terminal y
      | NonTerminal x, NonTerminal y -> ret @@ T.compare x y
      | Terminal _, NonTerminal _ -> ret @@ -1
      | NonTerminal _, Terminal _ -> ret @@ 1

  end

  (* rule right hand side *)
  module Rhs = struct
    type t = Symbol.t list
    let rec compare l1 l2 =
      match l1,l2 with
        [], [] -> 0
      | [], _ -> -1
      | _, [] -> 1
      | x::xs,y::ys -> match Symbol.compare x y with
                         0 -> compare xs ys
                       | c -> c
  end

  (* Earley 'item' *)
  module Item = struct
    type t = int * Rhs.t

    let compare (k1,l1) (k2,l2) =
      match compare k1 k2 with
        0 -> Rhs.compare l1 l2
      | r -> r

  end

  module IntOrd = struct type t = int let compare = compare end

  module RuleMap = MultiMap.Make(Symbol)(Rhs)

  module ItemMap = MultiMap.Make(Symbol)(Item)

  (* result of a reduced item *)
  module ItemSymbol = struct
    type t = int * Symbol.t
    let compare (r1,t1) (r2,t2) =
      match compare r1 r2 with
        0 -> Symbol.compare t1 t2
      | c -> c
  end

  module SymbolMap = Map.Make(Symbol)
  module SymbolSet = Set.Make(Symbol)
  module ItemSymbolSet = Set.Make(ItemSymbol)

  type rulemap =  RuleMap.t

  type itemmap = ItemMap.t

  module IntMap = Map.Make(IntOrd)

  open Symbol

  (* rules pretty printing *)
  module PP = struct
    
    let rec seq ?(sep = Format.pp_print_cut) pp ppf = function
      | [] -> ()
      | v :: vs ->
         pp ppf v; if vs <> [] then (sep ppf (); seq ~sep pp ppf vs)

    let symbol = Symbol.pp

    let symbol_sep ppf () = fmt ppf "; @,"

    let symbolset ppf s =
      let elts = SymbolSet.elements s in
      fmt ppf "@[%a@]" (seq ~sep:symbol_sep symbol) elts

    let itemsymbolset sep ppf s =
      ItemSymbolSet.iter (fun (r,tk) -> fmt ppf "%a @@%d;" symbol tk r) s

    let rule_sep ppf () = fmt ppf "@ "

    let pp ppf (n,rhs) =
      fmt ppf "@[%a ::= %a@]" symbol n (seq ~sep:rule_sep symbol) rhs

    let rulemap ppf (rs : rulemap) =
      fmt ppf "@[<2>%a@]" (fun ppf ->
                           RuleMap.iter (fun r rhs -> fmt ppf "%a@." pp (r,rhs))) rs

    let item_sep ppf () = fmt ppf "@."
    let item ppf (r,(k,rhs)) = fmt ppf "%a @@%d" pp (r,rhs) k

    let itemmap ppf (rs : itemmap) =
      let elts = ItemMap.bindings rs in
      fmt ppf "@[%a@]" (seq ~sep:item_sep item) elts
  (*
      fmt ppf "@[<2>%a@]" (fun r i -> item (r,i)) ItemMap.iter (fun )) rs
   *)
  end

  let get_previous_item previous tk r =
    let itemmap =
      try IntMap.find r previous with Not_found ->
        let () = sfmt "itemmap #%d not found" r in
        failwith (Format.flush_str_formatter ()) in 
    try 
      ItemMap.find tk itemmap
    with Not_found ->
      let () = sfmt "rule for symbol %a not found in itemmap #%d" PP.symbol tk r in
      failwith (Format.flush_str_formatter ())

  let symbol_eq x y = Symbol.compare x y = 0

  module First = struct

    module Rev = MultiMap.Make(Symbol)(Symbol)

    (* first is the map of non terminals to the terminals which may appear first in their rules *)
    type t = SymbolSet.t SymbolMap.t
    type rmap = Rev.t

    let pp ppf sm = SymbolMap.iter (fun k s -> fmt ppf "@[%a -> %a@] :: @;" PP.symbol k PP.symbolset s) sm

    (* compute one step of the transitive closure of the symbol map [smap] *)
    let transitive_closure smap =
      let fld sy nts fsmap =
        let open SymbolSet in
        let merge sy' set =
          let c = try SymbolMap.find sy' fsmap with Not_found -> empty in
          let u = union set c in
          u in
        let nts' = fold merge nts nts in
        if SymbolSet.compare nts nts' = 0 then fsmap else SymbolMap.add sy nts' fsmap in
      SymbolMap.fold fld smap smap

    (* 'apply' the transitive closure of smapnt on smapt *)
    let tc2 smapnt smapt =
      let fld sy setnt fsmap =
        let open SymbolSet in
        let merge sy' set =
          let c = try SymbolMap.find sy' smapt with Not_found -> empty in
          let u = union set c in
          u in
        let sett = fold merge setnt empty in
        SymbolMap.add sy sett fsmap in
      SymbolMap.fold fld smapnt smapt

    let build rulemap : t =
      let rec build (smap : t) =
        let sm' = transitive_closure smap in
        efmt "sm  = @[%a@]@.sm' = @[%a@]@.%!" pp smap pp sm';
        if sm' == smap then sm' else build sm'
      in
      let fld k rhs maps =
        let add k v (sm,smt) =
          let add k v sm =
            try
              let s = SymbolMap.find k sm in
              let s' = SymbolSet.add v s in
              if s == s' then sm else SymbolMap.add k s' sm
            with Not_found -> SymbolMap.add k (SymbolSet.singleton v) sm in
          match k, v with
          | NonTerminal _, NonTerminal _ ->
             add k v sm, smt
          | NonTerminal _, Terminal _ ->
             sm, add k v smt
          | _ -> sm, smt in
        add k (List.hd rhs) maps in
      let fnt, ft = RuleMap.fold fld rulemap (SymbolMap.empty, SymbolMap.empty) in
      let fnt = build fnt in
      tc2 fnt ft

    let reverse smap : rmap =
      let add_set nt set rmap =
        SymbolSet.fold (fun t rmap -> Rev.add t nt rmap) set rmap in
      SymbolMap.fold add_set smap Rev.empty

    let pp_rev ppf rmap =
      let pp_set ppf set =
        Rev.Set.iter (fun t -> fmt ppf "%a," PP.symbol t) set in
      Rev.iter_set (fun t set -> fmt ppf "@[%a -> @[%a@]@]" PP.symbol t pp_set set) rmap

  end

  (* scanner:
   * returns sets of rules
   * - activated by the symbol sigma
   * - completed by the symbol
   *)
  let scan (itemmap : itemmap) (sigma : Symbol.t) =
    let accepted, remaining =
      ItemMap.partition (fun _ -> function (k,x::xs) -> symbol_eq x sigma | (k,[]) -> false) itemmap in
    efmt "scan : accepted = %a (%a)\n" PP.itemmap accepted PP.symbol sigma;
    let accepted = ItemMap.map (function (r,x::xs) -> (r,xs) | _ -> assert false) accepted in
    let rec split tk (r,l) (active, completed) =
      match l with
        [] -> active, ItemSymbolSet.add (r,tk) completed
      | x -> ItemMap.add tk (r,x) active, completed in
    let active, completed = ItemMap.fold split accepted (ItemMap.empty,ItemSymbolSet.empty) in
    active, completed, remaining

  (* completer:
   * reduces completed items to their (lhs) symbols, 
   * and scans for items expecting them
   * until there's no more completed items
   *)
  let rec complete active completed previous =
    if ItemSymbolSet.is_empty completed
    then active
    else let r, tk = ItemSymbolSet.choose completed in
         efmt "complete : choosing '%a'\n%!" PP.symbol tk;
         let completed = ItemSymbolSet.remove (r,tk) completed in
         let a, c, remaining = scan (IntMap.find r previous) tk in
         (*         let previous = IntMap.add r remaining previous in  *)
         complete (ItemMap.merge a active) (ItemSymbolSet.union completed c) previous

  (* predictor:
   * return the set of rules potentially activated (no look-ahead)
   *)
  let rec predict round (rules : rulemap) activable (predicted : itemmap) (active : itemmap) =
    let fld k (r,rhs) ts =
       match rhs with
         x::xs -> (*if not @@ First.Rev.Set.mem x activable then ts else *) SymbolMap.add x r ts
       | [] -> assert false in
    let ts = ItemMap.fold fld active SymbolMap.empty in
    if SymbolMap.is_empty ts
    then predicted
    else
      let p, rules = RuleMap.partition (fun k _ -> SymbolMap.mem k ts) rules in
      let p = RuleMap.fold (fun k rhs -> ItemMap.add k (round,rhs)) p ItemMap.empty in
      predict round rules activable (ItemMap.merge predicted p) p

  let parse (rules : rulemap) first start lexbuf =
    let rec parse round (previous : itemmap IntMap.t) (active : itemmap) lexbuf =
      efmt "active = @[%a@]\n" PP.itemmap active;
      match ItemMap.is_empty active, T.lexeme lexbuf with
      | _, None -> ()
      | true, _ -> failwith "no active rules"
      | _, Some lx ->
         let activable = First.Rev.find (Terminal lx) first in
         let predicted = predict round rules activable ItemMap.empty active in
         efmt "@[predicted = %a@]@.%!" PP.itemmap predicted;
         let itemmap = ItemMap.merge active predicted in
         efmt "@[itemmap #%i = @;%a@]@.%!" round PP.itemmap itemmap;
         let active, completed, remaining = scan itemmap (Terminal lx) in
         efmt "before completion\n%!";
         efmt "active = @[%a@]\n%!" PP.itemmap active;
         efmt "completed = @[%a@]\n%!" (PP.itemsymbolset ";") completed;
         efmt "remaining = @[%a@]\n%!" PP.itemmap remaining;
         let previous = IntMap.add round itemmap previous in
         let active = complete active completed previous in
         efmt "after completion\n%!";
         efmt "active = @[%a@]\n%!" PP.itemmap active;
         efmt "completed = @[%a@]\n%!" (PP.itemsymbolset ";") completed;
         prerr_endline "---------------";
         IntMap.iter (fun m im -> efmt "@[<2>previous #%d = @,%a@]\n%!" m PP.itemmap im) previous;
         parse (succ round) previous active lexbuf
    in
    let rhs = RuleMap.(find start rules |> Set.elements) in
    let items = List.map (fun rhs -> 1, rhs) rhs in
    let active = ItemMap.(add_list start items empty) in
    parse 1 (IntMap.singleton 1 active) active lexbuf

end


module Test = struct

  module T = struct
    type t = char
    type lexbuf = {mutable pos: int; data : string}
    let compare = compare
    let lexeme l =
      let p = l.pos in
      if p < String.length l.data then
        begin
          efmt "reading char '%c'\n%!" l.data.[p];
          l.pos <- succ p;
          Some l.data.[p]
        end
      else None
    let to_string = Char.escaped
  end

  module Parser = Make(T)
  open T
  open Parser

  let t x : Symbol.t = Symbol.Terminal x
  let nt x : Symbol.t = Symbol.NonTerminal x

  let mk_rulemap ruleset = List.fold_left (fun rs (tk,r) -> RuleMap.add_list tk r rs) RuleMap.empty ruleset

  let ruleset1 = [
    nt 'S', [ [nt 'E'] ];
    nt 'E', [ [nt 'E'; nt 'Q'; nt 'F']; [nt 'F'] ];
    nt 'F', [ [t 'a'] ];
    nt 'Q', [ [t '+']; [t '-'] ];
  ]
  let rulemap1 = mk_rulemap ruleset1
  let first1 = First.(build rulemap1 |> reverse)

  let ruleset2 = [
    nt 'T', [ [ nt 'S'] ];
    nt 'S' , [ [ nt 'A' ]; [ nt 'A'; nt 'B' ]; [ nt 'B' ]];
    nt 'A' , [ [ nt 'C' ] ];
    nt 'B' , [ [ nt 'D' ] ];
    nt 'C' , [ [ t 'p' ] ];
    nt 'D' , [ [ t 'q' ] ];
  ]
  let rulemap2 = mk_rulemap ruleset2
  let first2 = First.(build rulemap2 |> reverse)

  let _ =
    efmt "starting\n";
    efmt "first1 = @[%a@]\n" First.pp_rev first1;
    parse rulemap1 first1 (nt 'S') {pos=0;data="a+a-a"};
    efmt "----------------------------------------------------\n"; 
    efmt "first2 = @[%a@]\n" First.pp_rev first2;
    parse rulemap2 first2 (nt 'S') {pos=0;data="q"};

end
