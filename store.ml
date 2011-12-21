open OUnit

(** a simple soft-state data store. Initially holds a map of names to (value, 
    optional sub-store and timeout) *)

(** item in the store *)
type item = {
	(** name (unique) *)
	name : string;
	(** option sub-store *)
	(*store : t;*)
	(** json-encoded *)
	jsonvalue : string; 
	(** unix time *)
	expires : float option;
}

(** the store value type *)
type t = (string,item) Hashtbl.t

(** create store *)
let create : t = 
	Hashtbl.create 1 
	
(** put name/value in store; timeout is relative time in seconds *)
let put s name value timeout =
	let now = OS.Clock.time () in
	let expires = match timeout with
		  Some t -> Some (now+.t)
	  | None   -> None in
	let item = {name=name;jsonvalue=value;expires=expires} in
	Hashtbl.replace s name item
	
let expired item now = match item.expires with
	  Some t -> t<=now
	| None   -> false
	
(** get value option for name *)
let get s name =
	try 
		let item = Hashtbl.find s name in
	  let now = OS.Clock.time () in
		if (expired item now) then (Hashtbl.remove s name; None) 
		else Some item.jsonvalue
	with Not_found -> None

(** list values - no expire *)
let list s =
	let get_names name item names =
		name :: names in
	Hashtbl.fold get_names s []

(** remove any binding; return old value *)
let remove s name =
	let value = get s name in
	Hashtbl.remove s name;
	value

(** purge expired values; return next expiry time *)
let get_expires s =
	let now = OS.Clock.time () in
	let find_expired name item namelist = 
		if (expired item now) then name :: namelist else namelist in
	let expired = Hashtbl.fold find_expired s [] in
	List.iter (fun name -> Hashtbl.remove s name) expired;
	let min_expires name item expires =
		match expires with
			| None -> item.expires 
			| Some m -> match item.expires with 
				| None -> Some m
				| Some t -> Some (min m t) 
	in
	Hashtbl.fold min_expires s None

(** tests... *)
let test_store () = 
	let store = create in
	put store "a" "aval" None;
	store
	 
let test_empty _ =
	let store = test_store () in
	assert_equal None (get store "b")

let test_get _ =
	let store = test_store () in
	assert_equal (Some "aval") (get store "a")

let test_not_expire _ =
	let store = test_store () in
	put store "b" "bval" (Some 2.);
	OS.Time.sleep 1. >>
	assert_equal (Some "bval") (get store "b") >>
	assert_equal ["a";"b"] (List.sort compare (list store)) >>
	match get_expires store with 
		| None -> assert_failure "no expire time returned"
		| Some t -> OS.Console.log_s (Printf.sprintf "expires %f vs %f" t (OS.Clock.time ()))
			
let test_expire _ =
	let store = test_store () in
	put store "b" "bval" (Some 1.);
	OS.Time.sleep 2. >>
	assert_equal None (get store "b")

let test_expire2 _ =
	let store = test_store () in
	put store "b" "bval" (Some 1.);
	OS.Time.sleep 2. >>
	assert_equal ["a";"b"] (List.sort compare (list store)) >>
	match get_expires store with 
		| None -> OS.Console.log "no expire time"; Lwt.return ()
		| Some t -> assert_failure "expire time returned" >>
	assert_equal ["a"] (List.sort compare (list store)) 

let test_replace _ =
	let store = test_store () in
	put store "a" "aval2" None;
	assert_equal (Some "aval2") (get store "a")

let test_remove _ =
	let store = test_store () in
	assert_equal (Some "aval") (remove store "a") >>
	assert_equal None (get store "a")

let test_list _ =
	let store = test_store () in
	put store "b" "bval" None;
	let names = List.sort compare (list store) in
	assert_equal ["a";"b"] names

let suite = "Store test" >::: ["test_empty" >:: test_empty;
	"test_get" >:: test_get; 
	"test_not_expire" >:: test_not_expire; 
	"test_expire" >:: test_expire;
	"test_expire2" >:: test_expire2;
	"test_replace" >:: test_replace;
	"test_remove" >:: test_remove;
	"test_list" >:: test_list ]
	
let main() = 
	run_test_tt suite >> 
	OS.Console.log_s "done" >> Lwt.return ()
		