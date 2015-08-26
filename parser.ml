
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


module type SYMBOL = sig

    type token
    type t
    val lift : token -> t
    val compare : t -> t -> int
    val is_terminal : t -> bool
    val pp : Format.formatter -> t -> unit

end

module Make(Token : TOKEN) (Symbol : SYMBOL with type token = Token.t) = struct

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

  (* Earley 'item':
   * the integer is the parsing round the item was generated from
   * the symbol is the rule lhs, ie. the symbol it reduces to
   * the first 'rhs' is the recognised part so far (ie the left side of the dot)
   * the second 'rhs' is the remaining of the rule *)
  module Item = struct

    type t = int * Symbol.t * Rhs.t * Rhs.t

    let compare (k1,s1,l1,r1) (k2,s2,l2,r2) =
      match compare k1 k2, Symbol.compare s1 s2, Rhs.compare l1 l2, Rhs.compare r1 r2 with
      | 0, 0, 0, r
      | 0, 0, r, _
      | 0, r, _, _
      | r, _, _, _ -> r

  end

  module IntOrd = struct type t = int let compare = compare end

  module RuleMap = MultiMap.Make(Symbol)(Rhs)

  module ItemMap = MultiMap.Make(Symbol)(Item)
  module ItemSet = ItemMap.Set

  module SymbolMap = Map.Make(Symbol)
  module SymbolSet = Set.Make(Symbol)

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


    let completed ppf (r,sy) = fmt ppf "%a @@%d" symbol sy r

    let completed_list ppf l = seq ~sep:symbol_sep completed ppf l

    let rule_sep ppf () = fmt ppf "@ "

    let rhs ppf r =
      seq ~sep:rule_sep symbol ppf r

    let pp ppf (n,r) =
      fmt ppf "@[%a ::= %a@]" symbol n rhs r

    let rulemap ppf (rs : rulemap) =
      fmt ppf "@[<2>%a@]"
          (fun ppf -> RuleMap.iter (fun r rhs -> fmt ppf "%a@." pp (r,rhs))) rs

    let item_sep ppf () = fmt ppf "@."

    let item ppf (k,n,rhs1, rhs2) = fmt ppf "%a ::= %a . %a @@%d" symbol n rhs (List.rev rhs1)  rhs rhs2 k

    let item_seq ppf l = seq ~sep:item_sep item ppf l

    let itemset tk ppf set =
      let elts = ItemSet.elements set in
      fmt ppf "@[%a@]" item_seq @@ List.map (fun (k,n,r1,r2) -> k,n,r1,tk::r2) elts

    let itemmap ppf (rs : itemmap) =
      let elts = ItemMap.bindings rs in
      fmt ppf "@[%a@]" item_seq @@ List.map (fun (tk, (k,n,r1,r2)) -> k,n,r1,tk::r2) elts

  end

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
        (*        efmt "sm  = @[%a@]@.sm' = @[%a@]@.%!" pp smap pp sm'; *)
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
          match Symbol.is_terminal k, Symbol.is_terminal v with
          | false, false ->
             add k v sm, smt
          | false, true ->
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
        Rev.Set.iter (fun t -> fmt ppf ",%a" PP.symbol t) set in
      Rev.iter_set (fun t set -> fmt ppf "@[%a -> @[%a@];@;@]" PP.symbol t pp_set set) rmap

  end

  let shift x = function (k,s,l,r) -> (k,s,x::l,r)

  (* scanner:
   * returns sets of rules
   * - activated by the symbol sigma
   * - completed by the symbol
   *)
  let scan (itemmap : itemmap) (sigma : Symbol.t) =
    let accepted, remaining =
      try 
        let a = ItemMap.find sigma itemmap in
        a, ItemMap.remove sigma itemmap
      with Not_found -> ItemSet.empty, itemmap
    in
    efmt "scan : accepted = %a (%a)\n" (PP.itemset sigma) accepted PP.symbol sigma;
    let accepted : Item.t list = ItemSet.elements accepted |> List.map (shift sigma) in
    let rec split (active, completed) ((k,n,l,r): Item.t) =
      match r with
        [] -> active, (k,n)::completed
      | tk::xs -> ItemMap.add tk (k,n,l,xs) active, completed in
    let active, completed = List.fold_left split (ItemMap.empty,[]) accepted in
    active, completed, remaining

  (* completer:
   * reduces completed items to their (lhs) symbols, 
   * and scans for items expecting them
   * until there's no more completed items
   *)
  let rec complete active completed previous =
    match completed with
      [] -> active
    | (r,tk)::completed ->
       efmt "complete : choosing '%a' (%d)\n%!" PP.symbol tk r;
       let a, c, remaining = scan (IntMap.find r previous) tk in
       complete (ItemMap.merge a active) (List.append completed c) previous

  let add_rules round itemmap rules sset =
    let p, rules = RuleMap.partition (fun k _ -> SymbolSet.mem k sset) rules in
    let p = RuleMap.fold (fun k -> function (x::rhs) -> ItemMap.add x (round,k,[],rhs) | _ -> assert false) p ItemMap.empty in
    p, rules

  (* predictor:
   * return the set of rules potentially activated (no look-ahead)
   *)
  let rec predict round (rules : rulemap) activable (predicted : itemmap) (active : itemmap) =
    let fld tk (k,n,l,r) ts =
      if not @@ First.Rev.Set.mem tk activable then ts else SymbolSet.add tk ts in
    let ts = ItemMap.fold fld active SymbolSet.empty in
    if SymbolSet.is_empty ts
    then predicted
    else
      let p, rules = add_rules round ItemMap.empty rules ts in
      predict round rules activable (ItemMap.merge predicted p) p

  let parse (rules : rulemap) first start lexbuf =
    let rec parse round (previous : itemmap IntMap.t) (active : itemmap) lexbuf =
      efmt "active = @[%a@]\n" PP.itemmap active;
      match ItemMap.is_empty active, Token.lexeme lexbuf with
      | _, None -> ()
      | true, _ -> failwith "no active rules"
      | _, Some lx ->
         let activable = try First.Rev.find (Symbol.lift lx) first with Not_found -> failwith @@ Format.(sfmt "unhandled lexeme %s" (Token.to_string lx); flush_str_formatter ()) in
         efmt "@[activable = %a@]@.%!" (fun ppf set -> First.Rev.Set.iter (fun sy -> fmt ppf "%a;@;" PP.symbol sy) set) activable;
         let predicted = predict round rules activable ItemMap.empty active in
         efmt "@[predicted = %a@]@.%!" PP.itemmap predicted;
         let itemmap = ItemMap.merge active predicted in
         efmt "@[itemmap #%i = @;%a@]@.%!" round PP.itemmap itemmap;
         let active, completed, remaining = scan itemmap (Symbol.lift lx) in
         efmt "before completion\n%!";
         efmt "active = @[%a@]\n%!" PP.itemmap active;
         efmt "completed = @[%a@]\n%!" PP.completed_list completed;
         efmt "remaining = @[%a@]\n%!" PP.itemmap remaining;
         let previous = IntMap.add round itemmap previous in
         let active = complete active completed previous in
         efmt "after completion\n%!";
         efmt "active = @[%a@]\n%!" PP.itemmap active;
         efmt "completed = @[%a@]\n%!" PP.completed_list completed;
         prerr_endline "---------------";
         IntMap.iter (fun m im -> efmt "@[<2>previous #%d = @,%a@]\n%!" m PP.itemmap im) previous;
         parse (succ round) previous active lexbuf
    in
    let active, _ = add_rules 1 ItemMap.empty rules (SymbolSet.singleton start) in
    parse 1 (IntMap.singleton 1 active) active lexbuf

end

module Test = struct

  (* lexical token *)
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

  (* grammar symbol *)
  module Symbol = struct

    type token = char

    type t =  
        Terminal of token
      | NonTerminal of char

    let lift x = Terminal x

    let is_terminal = function Terminal _ -> true | _ -> false

    let compare a b =
      match a, b with
        Terminal x, Terminal y
      | NonTerminal x, NonTerminal y -> T.compare x y
      | Terminal _, NonTerminal _ -> -1
      | NonTerminal _, Terminal _ -> 1

    let pp ppf x =
      let k, x =
        match x with Terminal x -> "t", x | NonTerminal x -> "nt", x in
      fmt ppf "%s" (T.to_string x)

  end


  module Parser = Make(T)(Symbol)
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
    parse rulemap2 first2 (nt 'T') {pos=0;data="q"};

end
