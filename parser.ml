
let fmt ppf = Format.fprintf ppf

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
      (*
    val cardinal : 'a t -> int
    val bindings : 'a t -> (key * 'a) list
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

(*      
    let bindings : 'a t -> (key * 'a) list
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
  type token =
      Terminal of id
    | NonTerminal of id

  (*
  (* grammar rule *)
  type rule = token * token list

  (* Earley parser item: rule which has been partially parsed *)
  type item = rule * token list
   *)

  let tk_cmp a b =
    match a, b with
      Terminal x, Terminal y
    | NonTerminal x, NonTerminal y -> T.compare x y
    | Terminal _, NonTerminal _ -> -1
    | _ -> 1

  module TokenOrd = struct
    type t = token
    let compare = tk_cmp
  end

  type rhs = token list

  module RhsOrd = struct
    type t = rhs
    let rec compare l1 l2 =
      match l1,l2 with
        [], [] -> 0
      | [], _ -> -1
      | _, [] -> 1
      | x::xs,y::ys -> match tk_cmp x y with
                         0 -> compare xs ys
                       | c -> c
  end

  module RuleMap = MultiMap.Make(TokenOrd)(RhsOrd)
  module TokenSet = Set.Make(TokenOrd)

  type ruleset =  RuleMap.t

  type itemset = ruleset

  (* rules pretty printing *)
  let rec pp_seq sep pp ppf = function
      [] -> ()
    | x::xs -> fmt ppf "%a%s%a" pp x sep (pp_seq sep pp) xs


  let pp_token ppf x =
    let k, x =
      match x with Terminal x -> "t", x | NonTerminal x -> "nt", x in
    fmt ppf "%s %s" k (T.to_string x)

  let pp_tokenset sep ppf s =
    TokenSet.iter (fun tk -> fmt ppf "%a;" pp_token tk) s
    
  let pp ppf (n,rhs) =
    fmt ppf "@[%a : %a@]" pp_token n (pp_seq " " pp_token) rhs

  let pp_set ppf (rs : ruleset) =
    RuleMap.iter (fun r rhs -> fmt ppf "%a@." pp (r,rhs)) rs

  (* scanner:
   * returns sets of rules
   * - activated by the token sigma
   * - completed by the token
   *)
  let scan (itemset : itemset) (sigma : token) =
    let eq x y = tk_cmp x y = 0 in
    let accepted, remaining =
      RuleMap.partition (fun _ -> function (x::xs) -> eq x sigma | [] -> false) itemset in
    efmt "scan : accepted = %a (%a)\n" pp_set accepted pp_token sigma;
    let accepted = RuleMap.map (function (x::xs) -> xs | _ -> assert false) accepted in
    let rec split tk l (active, completed) =
      match l with
        [] -> active, TokenSet.add tk completed
      | x -> RuleMap.add tk x active, completed in
    let active, completed = RuleMap.fold split accepted (RuleMap.empty,TokenSet.empty) in
    active, completed, remaining

(*
  let merge k a b =
    match a, b with
      Some x, None
    | None, Some x -> Some x
    | _ -> assert false
 *)
  (* completer:
   * reduces rules completed to their token, 
   * and scans for items expecting it
   * until there's no more completed rules
   *)
  let rec complete active completed remaining =
    if TokenSet.is_empty completed
    then active, remaining
    else let tk = TokenSet.choose completed in
         let completed = TokenSet.remove tk completed in
         let a, c, r = scan remaining tk in
         complete (RuleMap.merge a active) (TokenSet.union completed c) (RuleMap.merge remaining r) 

  (* predictor:
   * return the set of rules potentially activated (no look-ahead)
   *)
  let rec predict (rules : ruleset) (predicted : itemset) (active : itemset) =
    let ts = RuleMap.fold (fun k rhs l -> TokenSet.add (List.hd rhs) l) active TokenSet.empty in
    if TokenSet.is_empty ts
    then predicted
    else
      let p, rules = RuleMap.partition (fun k _ -> TokenSet.mem k ts) rules in
      predict rules (RuleMap.merge predicted p) p

  let parse (rules : ruleset) start lexbuf =
    let rec parse (rules : ruleset) (active : itemset) lexbuf =
      efmt "active = @[%a@]\n" pp_set active;
      match T.lexeme lexbuf with
        None -> ()
      | Some lx ->
         let predicted = predict rules RuleMap.empty active in
         efmt "predicted = @[%a@]\n" pp_set predicted;
         let itemset = RuleMap.merge active predicted in
         efmt "itemset = @[%a@]\n" pp_set itemset;
         let active, completed, remaining = scan itemset (Terminal lx) in
         efmt "before completion\n";
         efmt "active = @[%a@]\n" pp_set active;
         efmt "completed = @[%a@]\n" (pp_tokenset ";") completed;
         efmt "remaining = @[%a@]\n" pp_set remaining;
         let active, remaining = complete active completed remaining in
         efmt "after completion\n";
         efmt "active = @[%a@]\n" pp_set active;
         efmt "completed = @[%a@]\n" (pp_tokenset ";") completed;
         efmt "remaining = @[%a@]\n" pp_set remaining;
         parse rules (RuleMap.merge active remaining) lexbuf
    in
    let active : itemset = RuleMap.filter (fun k _ -> k = start) rules in
    parse rules active lexbuf

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

  let t x : token = Terminal x
  let nt x : token = NonTerminal x

  let ruleset = [
    nt 'S', [ [nt 'E'] ];
    nt 'E', [ [nt 'E'; nt 'Q'; nt 'F']; [nt 'F'] ];
    nt 'F', [ [t 'a'] ];
    nt 'Q', [ [t '+']; [t '-'] ];
  ]
  let ruleset = List.fold_left (fun rs (tk,r) -> RuleMap.add_list tk r rs) RuleMap.empty ruleset

  let _ =
    efmt "starting\n";
    parse ruleset (nt 'S')  {pos=0;data="a+a-a"}

end
