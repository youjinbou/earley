
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
      type t
      val empty : t
      val is_empty : t -> bool
      val mem : key -> t -> bool
      val add : key -> value -> t -> t
      val add_list : key -> value list -> t -> t
      val singleton : key -> value -> t
      val remove : key -> t -> t
      val merge  : t -> t -> t
                                      (*
      val compare :
        ('a -> 'a -> int) -> 'a t -> 'a t -> int
      val equal :
        ('a -> 'a -> bool) -> 'a t -> 'a t -> bool
                                       *)
      val iter : (key -> value -> unit) -> t -> unit
      val fold :
        (key -> value -> 'b -> 'b) -> t -> 'b -> 'b
      val for_all : (key -> value -> bool) -> t -> bool
      val exists : (key -> value -> bool) -> t -> bool
      val filter : (key -> value -> bool) -> t -> t
      val partition :
        (key -> value -> bool) -> t -> t * t
      val bindings : t -> (key * value) list
      (*
    val cardinal : 'a t -> int
    val min_binding : 'a t -> key * 'a
    val max_binding : 'a t -> key * 'a
    val choose : 'a t -> key * 'a
    val split : key -> 'a t -> 'a t * 'a option * 'a t
       *)
      val find : key -> t -> value list
      val map : (value -> value) -> t -> t
      val mapi : (key -> value -> value) -> t -> t
    end

  module Make(OK : OrderedType) (OV : OrderedType) : S with type key = OK.t
                                                        and type value = OV.t = struct

    type key = OK.t
    type value = OV.t

    module M = Map.Make(OK)
    module S = Set.Make(OV)

    type t = S.t M.t

    let empty = M.empty
    let is_empty = M.is_empty

    let mem k m = M.mem k m

    let add k x m =
      try let xl = M.find k m in
          M.add k (S.add x xl) m
      with Not_found -> M.add k (S.singleton x) m

    let add_list k x m =
      let xl =
        try M.find k m
        with Not_found -> S.empty in
      M.add k (List.fold_right S.add x xl) m

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
    let find k m = S.elements @@ M.find k m

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

    let compare a b =
      match a, b with
        Terminal x, Terminal y
      | NonTerminal x, NonTerminal y -> T.compare x y
      | Terminal _, NonTerminal _ -> -1
      | _ -> 1

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

  module ItemSymbol = struct
    type t = int * Symbol.t
    let compare (r1,t1) (r2,t2) =
      match compare r1 r2 with
        0 -> Symbol.compare t1 t2
      | c -> c
  end

  module SymbolSet = Set.Make(Symbol)
  module SymbolMap = Map.Make(Symbol)
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


    let symbol ppf x =
      let k, x =
        match x with Terminal x -> "t", x | NonTerminal x -> "nt", x in
      fmt ppf "%s" (T.to_string x)

    let symbolset sep ppf s =
      SymbolSet.iter (fun tk -> fmt ppf "%a;" symbol tk) s

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

  (* scanner:
   * returns sets of rules
   * - activated by the symbol sigma
   * - completed by the symbol
   *)
  let scan (itemmap : itemmap) (sigma : Symbol.t) =
    let eq x y = Symbol.compare x y = 0 in
    let accepted, remaining =
      ItemMap.partition (fun _ -> function (k,x::xs) -> eq x sigma | (k,[]) -> false) itemmap in
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
  let rec predict round (rules : rulemap) (predicted : itemmap) (active : itemmap) =
    let fld k (r,rhs) ts =
       match rhs with
         x::xs -> SymbolMap.add x r ts
       | [] -> assert false in
    let ts = ItemMap.fold fld active SymbolMap.empty in
    if SymbolMap.is_empty ts
    then predicted
    else
      let p, rules = RuleMap.partition (fun k _ -> SymbolMap.mem k ts) rules in
      let p = RuleMap.fold (fun k rhs -> ItemMap.add k (round,rhs)) p ItemMap.empty in
      predict round rules (ItemMap.merge predicted p) p

  let parse (rules : rulemap) start lexbuf =
    let rec parse round (previous : itemmap IntMap.t) (active : itemmap) lexbuf =
      efmt "active = @[%a@]\n" PP.itemmap active;
      match T.lexeme lexbuf with
        None -> ()
      | Some lx ->
         let predicted = predict round rules ItemMap.empty active in
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
    let rhs = RuleMap.find start rules in
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

  let ruleset = [
    nt 'S', [ [nt 'E'] ];
    nt 'E', [ [nt 'E'; nt 'Q'; nt 'F']; [nt 'F'] ];
    nt 'F', [ [t 'a'] ];
    nt 'Q', [ [t '+']; [t '-'] ];
  ]
  let rulemap = List.fold_left (fun rs (tk,r) -> RuleMap.add_list tk r rs) RuleMap.empty ruleset

  let _ =
    efmt "starting\n";
    parse rulemap (nt 'S') {pos=0;data="a+a-a"}

end
