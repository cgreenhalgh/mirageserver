open OUnit
open Lwt

(** a simple soft-state data store. Initially holds a map of names to (value, 
    optional sub-store and timeout). Assumes that any operation might block. *)

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

(** create store - may block *)
let create = 
	return (Hashtbl.create 1)
	
(** put name/value in store; timeout is relative time in seconds - may block *)
let put s name value timeout = 
	let now = OS.Clock.time () in
	let expires = match timeout with
		  Some t -> Some (now+.t)
	  | None   -> None in
	let item = {name=name;jsonvalue=value;expires=expires} in
	return (Hashtbl.replace s name item)
	
let expired item now = match item.expires with
	  Some t -> t<=now
	| None   -> false
	
(** get item option for name - may block *)
let getitem s name =
	try 
		let item = Hashtbl.find s name in
	  let now = OS.Clock.time () in
		if (expired item now) then (Hashtbl.remove s name; return None) 
		else return (Some item)
	with Not_found -> return None

(** get value option for name - may block *)
let get s name =
	match_lwt getitem s name with
		| Some item -> return (Some item.jsonvalue)
		| None      -> return None

(** list values - no expire - may block *)
let list s =
	let get_names name item names =
		name :: names in
	return (Hashtbl.fold get_names s [])

(** remove any binding; return old value - may block *)
let remove s name =
	lwt value = get s name in
	Hashtbl.remove s name;
	return value

(** purge expired values; return next expiry time - may block *)
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
	return (Hashtbl.fold min_expires s None)

(** tests... *)
let test_store () = 
	lwt store = create in
	put store "a" "aval" None >>
	return store
	 
let test_empty _ =
	lwt store = test_store () in
	lwt res = get store "b" in
	assert_equal None res

let test_get _ =
	lwt store = test_store () in
	lwt res = get store "a" in
	assert_equal (Some "aval") res

let test_not_expire _ =
	lwt store = test_store () in
	put store "b" "bval" (Some 2.) >>
	OS.Time.sleep 1. >>
	lwt res = get store "b" in
	assert_equal (Some "bval") res >>
	lwt res = list store in
	assert_equal ["a";"b"] (List.sort compare res) >>
	match_lwt get_expires store with 
		| None -> assert_failure "no expire time returned"
		| Some t -> OS.Console.log_s (Printf.sprintf "expires %f vs %f" t (OS.Clock.time ()))
			
let test_expire _ =
	lwt store = test_store () in
	put store "b" "bval" (Some 1.) >>
	OS.Time.sleep 2. >>
	lwt res = get store "b" in
	assert_equal None res

let test_expire2 _ =
	lwt store = test_store () in
	put store "b" "bval" (Some 1.) >>
	OS.Time.sleep 2. >>
	lwt res = list store in
	assert_equal ["a";"b"] (List.sort compare res) >>
	match_lwt get_expires store with 
		| None -> OS.Console.log "no expire time"; Lwt.return ()
		| Some t -> assert_failure "expire time returned" >>
	lwt res = list store in 
	assert_equal ["a"] (List.sort compare res) 

let test_replace _ =
	lwt store = test_store () in
	put store "a" "aval2" None >>
	lwt res = get store "a" in
	assert_equal (Some "aval2") res

let test_remove _ =
	lwt store = test_store () in
	lwt res = remove store "a" in
	assert_equal (Some "aval") res >>
	lwt res = get store "a" in
	assert_equal None res

let test_list _ =
	lwt store = test_store () in
	put store "b" "bval" None >>
	lwt res = list store in
	let names = List.sort compare res in
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
		