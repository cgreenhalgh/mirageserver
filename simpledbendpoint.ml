(* Endpoint abstraction for a simple (no-sql) datastore. That's the idea, 
 * anyway... 
 *
 * V.1
 * A value can be large; it is stored in a series of dedicated blocks.
 * The blocks are identified (block numbers) in a value-leaf-node. 
 * Block 0 is an end marker.
 * Where there are too many blocks to list in one value-leaf-node,
 * a hierarchy of value-nodes are built.
 * 
 * A key is at most 1024 bytes. An entry-node holds the ref for the
 * root value-[leaf-]node, the first 8 bytes of the key and an in-page
 * ref to the rest of the key (else 0), a timestamp (seconds) and sequence 
 * number (update).
 * 
 * Entries are organised in a balanced entry-tree, with first 8 bytes of
 * the key (index value), ref to rest (in page), left ref and right ref.
 * 
 * Structured pages comprise a sequence a sequence of nodes 
 * starting at offset 0, terminated by 0:16, and a sequence of 
 * key/value byte sequences,  starting at offset pagesize-2, with highest two
 * bytes = size including size (:) i.e. 2 bytes), ending with 0:16.
 * 
 * Each page starts with 2 byte page type:
 *  1 = root page (structured page)
 *  2 = node page (structured page)
 *  3 = value whole-page
 *  4 = page usage map - 4 bytes/page, starting from that page; first at page 0 ?!
 *
 * Followed by a 2 byte page size log2
 *
 * Each node starts with a 2-byte ID:
 *  0 = end-node
 *  1 = root-node
 *  256 = value-leaf-node
 *  257- = value-nodes of varying levels
 *  2 = entry node
 *  512- = b-tree-nodes of varying levels
 * 
 * Page ID:
 *  1 = structured page, v.1
 * 
 * value-leaf-node = BITSTRING {
 *   size [20] : 16 : unsigned;
 *   nodetype [1] : 16 : unsigned; 
 *   page0pad [0] : 2;
 *   page0 : 30 : unsigned;
 *   page1pad : 2
 *   page1 : 30 : unsigned;
 * }
 * Total = 20 bytes.
 *
 * value-node = BITSTRING {
 *   size [16] : 16 : unsigned;
 *   nodetype [2-32] : 16 : unsigned; 
 *   vnode0pad : 2;
 *   vnode0page : 30 : unsigned;
 *   vnode0offset : 16;
 *   vnode1pad : 2;
 *   vnode1page : 30 : unsigned;
 *   vnode1offset : 16: unsigned;
 * }
 * Total = 16
 * 
 * entry-node = BITSTRING {
 *   size [32] : 16 : unsigned;
 *   nodetype [2] : 16 : unsigned; 
 *   keyprefix : 64 : bitstring;
 *   keyoffset : 16 : unsigned;
 *   0 : 2;
 *   valuepage : 30 : unsigned;
 *   valueoffset : 16 : unsigned;
 *   timestamp : 32;
 *   sequence : 64;
 * }
 * Total = 32
 * 
 * btree-node = BITSTRING {
 *   size [26] : 16 : unsigned;
 *   nodetype [] : 16 : unsigned;
 *   keyprefix : 64 : bitstring;
 *   keyoffset : 16 : unsigned;
 *   0 : 2;
 *   lpage : 30 : unsigned;
 *   loffset : 16;
 *   0 : 2;
 *   rpage : 30 : unsigned;
 *   roffset : 16: unsigned;
 * }
 * Total = 26
 * 
 * Key continuations in the top part of the page are stored as bytes followed by 
 * length : 16 : unsigned, with the last length being in the last 2 bytes of the page. 
 *
 * The first page includes: page (30) and offset (16) of current root btree-node:
 * root-node = BITSTRING {
 *   size [] : 16 : unsigned;
 *   nodetype [0] : 16; 
 *   0x513913DBL : 32 (magic);
 *   version : 16; - currently 1
 *   0 : 2;
 *   rootpage : 30 : unsigned;
 *   rootoffset : 16 : unsigned;
 *   lastpage : 30 : unsigned; (* last page in use *)
 *   reserved...
 * }
 * Total = 10+reserved = 32
 * 
 * Chris Greenhalgh, University of Nottingham, 2012-01-04.
 *)

open Endpoint
open Lwt
open Printf
open Bitstring

(** bare key/value store interface, as a class *)
type simpledb = <
  (** id = blkif name *)
  id : string;
  (** read a value (key) (no metadata returned) *)
  recv : string -> bytestreamin inresult Lwt.t;
	(** write/update; close (complete) when done which will auto-send (no metadata 
   * returned or needed for send) *)
  getbuf : string -> bytestreamout Lwt.t;
>
(** internal state *)
type t = {
	id : string;
	blkep : Blkifendpoint.blkendpoint;
	(* ... ? *)	
}
	
(** recv implementation *)
let _recv t (key:string) : bytestreamin inresult Lwt.t =
	(* no *) 
	return Complete
	
(** getbuf implementation *)
let _getbuf t (key:string) : bytestreamout Lwt.t =
	(* bytestreamout *)	
	return (object
	  method getbuf size : (Bitstring.t * unit * bytestreamoutbuf) Lwt.t =
			raise_lwt (Failure "unimplemented");
    method discardout () = ()
	end)
	
(** get simpledb over specified blkif *)
let get name : simpledb Lwt.t =
  lwt blkep = (Blkifendpoint.get "test") in 
  OS.Console.log (sprintf "Got simpledb blkif name %s, sector size %d" blkep#id blkep#block_size);
	let t = { id = blkep#id; blkep = blkep } in
  return (object
	  method id = blkep#id;
		method recv key : bytestreamin inresult Lwt.t = _recv t key ;
		method getbuf key : bytestreamout Lwt.t = _getbuf t key;
	end)
	
(** page header record *)
type pageheader = {
	pagetype : int;
	pagesizelog2 : int
}

(** page header size, bytes *)
let pageheadersize = 4
(** root page type *)
let rootpagetype = 1
let nodepagetype = 2
let valuepagetype = 3
	
(** root node size *)
let rootnodesize = 32
(** root node type *)
let rootnodetype = 1
(** magic *)
let magic = 0x513913DBl
(** version *)
let version = 1

type nodeheader = {
	size : int;
	nodetype : int
}

(** root node record *)
type rootnode = {
	(* after common header *)
  magic : int32;
  version : int;
  rootpage : int;
  rootoffset : int;
  lastpage: int;
}

(** entry node type *)
let entrynodetype = 2

(** bits of key in prefix (bytes) *)
let keyprefixlength = 8
(** length of entrynode record *)
let entrynodesize = 32
(** entry node record *)
type entrynode = {
  (* after common header *)
  ekeyprefix : bitstring;
  ekeyoffset : int;
  valuepage : int;
  valueoffset : int;
  timestamp : int32;
  sequence : int64;
}

type nodebody = Rootnode of rootnode
 | Entrynode of entrynode
 | Unknownnode
 
(** info about extra key info at end of page *)
type extrakey = {
	(** offset is page offset of the size word, at the end of the extrakey *)
	koffset : int;
	(** bytes *)
	ksize : int;
	extra : bitstring
}

(** info about a node *)
type node = {
	noffset : int;
	header : nodeheader;
	body : nodebody
}

(** page cache entry *)
type page = {
	(** which page *)
  page : int;
	(** page header *)
  pageheader : pageheader;
	(** page data *)
  data : bitstring;
	(** the nodes - bottom up*)
  nodes : node list;
	(** the extra keys - top down *)
	extrakeys : extrakey list;
	(** offset of 'end' (type 0) node found *)
	pnodeoffset : int;
	(** offset of 'end' (size 0) extrakey found *)
  pextrakeyoffset : int
}

(** parse page header *)
let parse_pageheader data : pageheader =
	bitmatch data with 
		{
			 pagetype : 16 : unsigned, littleendian;
		   pagesizelog2 : 16 : unsigned, littleendian
		} -> { pagetype=pagetype; pagesizelog2=pagesizelog2 }

(** bitstring of page header *)
let bitstring_of_pageheader ph = BITSTRING {
	ph.pagetype : 16 : unsigned, littleendian;
	ph.pagesizelog2 : 16 : unsigned, littleendian
}

(** try to parse a node - any type. data starts at current offset! *)
let parse_node data offset size nodetype : node =
	(* skip header *)
	let data = subbitstring data (2*16) (8*(size-4)) in
	let header = { size = size; nodetype = nodetype } in
	if (nodetype = rootnodetype) then
		(* root node *)
		bitmatch data with 
    { 
			magic : 32 : bigendian; (* magic *)
      version : 16 : littleendian; (* version *)
      rootpage : 30 : unsigned, littleendian;
      0 : 2; (* pad *)
      rootoffset : 16 : unsigned, littleendian;
      lastpage : 30 : unsigned, littleendian        
     } -> { noffset = offset; header = header;
		        body = Rootnode { 
            magic = magic; 
            version = version;
            rootpage = rootpage;
            rootoffset = rootoffset;
            lastpage = lastpage
          } }
  else if (nodetype = entrynodetype) then
		(* entry node *)
		bitmatch data with 
		{
	    ekeyprefix : keyprefixlength*8 : bitstring; (* NB not sure if the actual length is important *)
      ekeyoffset : 16 : unsigned, littleendian;
      0 : 2;
      valuepage : 30 : unsigned, littleendian;
      valueoffset : 16 : unsigned, littleendian;
      timestamp : 32 : littleendian;
      sequence : 64 : littleendian
    } -> { noffset = offset; header = header;
           body = Entrynode { 
           ekeyprefix = ekeyprefix; 
           ekeyoffset = ekeyoffset;
				   valuepage = valuepage;
					 valueoffset = valueoffset;
					 timestamp = timestamp;
					 sequence = sequence
         } }
   else
		 raise (Failure (sprintf "Unknown node type %d" nodetype))	
				
(** try to parse a structured page *)
let parse_page page data : page =
	let pageheader = parse_pageheader data in
	(* data starts at current offset! *)
	let rec read_nodes data offset nodes = bitmatch data with
		{ size : 16 : unsigned, littleendian;
			nodetype : 16 : unsigned, littleendian
	  } -> 
			if (size*8 > bitstring_length data) then
	   	  raise (Failure (sprintf "node size too big (%d/%d)" (bitstring_length data) (size*8)))
      else if (size==0) then
			  (* end *) (nodes,offset)
			else begin
				let node = parse_node data offset size nodetype in
				let nodes = node :: nodes in
				read_nodes (dropbits (size*8) data) (offset+size) nodes
			end
	in
	(* parse nodes *)
	let (nodes,pnodeoffset) = read_nodes (dropbits (pageheadersize*8) data) pageheadersize [] in
	(* parse extra keys *)
	(* data is whole page *)
	let rec read_extrakeys pagedata offset extrakeys = bitmatch (dropbits (offset*8) pagedata) with
		{ size : 16 : unsigned, littleendian } ->
			if (size==0) then
				(* end *) (extrakeys, offset)
			else if (size>offset) then
				raise (Failure (sprintf "extrakey too big (%d/%d)" size offset))
			else begin
				let extra = subbitstring data ((offset-(size-2))*8) ((size-2)*8) in
				let extrakey = { koffset=offset; ksize=size; extra=extra } in
				read_extrakeys pagedata (offset-size) (extrakey :: extrakeys)
			end in
	let (extrakeys, pextrakeyoffset) = read_extrakeys data ((bitstring_length data)/8-2) [] in
	{ page = page; pageheader = pageheader;
	  data = data; nodes = nodes; pnodeoffset = pnodeoffset;
	  extrakeys = extrakeys; pextrakeyoffset = pextrakeyoffset }
		
(** create a rootnode for given root page & offset *)
let create_rootnode rootpage rootoffset lastpage = BITSTRING {
	  rootnodesize : 16 : unsigned, littleendian;
		rootnodetype : 16 : unsigned, littleendian;
		magic : 32 : bigendian; (* magic *)
		version : 16 : littleendian; (* version *)
		rootpage : 30 : unsigned, littleendian;
    0 : 2; (* pad *)
		rootoffset : 16 : unsigned, littleendian;
		lastpage : 30 : unsigned, littleendian; (* last page *)
    0 : 2 (* pad *)
		(* reserved *)
	}
	
let bitstring_of_entrynode n = 
	(*let keyprefix = if (bitstring_length n.key > keprefixlength) 
	   then takebits keprefixlength n.key 
		 else n.key in*)
	BITSTRING {
		entrynodesize : 16 : unsigned, littleendian;
		entrynodetype : 16 : unsigned, littleendian;
    n.ekeyprefix : keyprefixlength*8 : bitstring; (* actual length must match *)
    n.ekeyoffset : 16 : unsigned, littleendian;
    0 : 2;
    n.valuepage : 30 : unsigned, littleendian;
    n.valueoffset : 16 : unsigned, littleendian;
    n.timestamp : 32 : littleendian;
    n.sequence : 64 : littleendian
	}

let sizelog2 size = 
	let rec div2 size count = match size with
		| 0 -> count
		| s -> div2 (s/2) count+1
	in (div2 size (-1))

(** ininitialise a root page bitstring with a page header and root node, only *)
let create_rootpage size sizelog2 = 
	let rootheader = bitstring_of_pageheader 
  	{ pagetype=rootpagetype; pagesizelog2=sizelog2 } in 
	let rootnode = create_rootnode 0 0 0 in
	concat [ rootheader; rootnode; zeroes_bitstring (((size-pageheadersize)*8)-bitstring_length rootnode) ]

(** write page back; blocking *)
let write_page blkep page =
  (* getbuf to write block 0 *)
  lwt (outdata,(),outbuf) = blkep#blkout#getbuf page.page in
  (* copy read data to writing data *)
  bitstring_write page.data 0 outdata;
  (* write to disk *)
  outbuf#send () >>
  (* happy :) *)
  ( OS.Console.log (sprintf "wrote page %d" page.page);
    return () )

(** danger - clobber/initialise root page on specified device *)
let init_device name = 
	OS.Console.log (sprintf "WARNING: initialising block device %s as empty simpleDB" name);
	OS.Time.sleep 3.0 >>
  lwt blkep = (Blkifendpoint.get "test") in 
  OS.Console.log (sprintf "Got Blkif name %s, sector size %d" blkep#id blkep#block_size);
  let pagesizelog2 = sizelog2 blkep#block_size in
	let rootpage = create_rootpage blkep#block_size pagesizelog2 in
  (* getbuf to write block 0 *)
  lwt (outdata,(),outbuf) = blkep#blkout#getbuf 0 in
  (* copy read data to writing data *)
  bitstring_write rootpage 0 outdata;
  (* write to disk *)
  outbuf#send () >>
  (* happy :) *)
  ( OS.Console.log "written new root page";
    return () )

(** danger - clobber/initialise root page on "test"  vbd *)
let init_device_test () = init_device "test"

(** split key into fixed length prefix and option of variable length remainder *)
let split_key key =
	if (bitstring_length key > keyprefixlength*8) then
		(takebits (keyprefixlength*8) key, Some (dropbits (keyprefixlength*8) key))
	else if (bitstring_length key < keyprefixlength*8) then
    (* note: need to pad end of key to ensure 64 bits written *)
		(concat [ key; zeroes_bitstring (keyprefixlength*8-bitstring_length key) ], None)
	else
		(key, None)

(** extrakeylen required for node (bytes) *)
let extrakey_len key = 
    if (bitstring_length key > keyprefixlength*8) then
      (bitstring_length key)/8 - keyprefixlength
    else
			0

(** does page have enough space for a new node? 
    (node-size, extrakeylen[bytes]) -> bool *)
let is_room_for_node page nodesize extrakeylen =
	(* need at least 0:16 in the free gap *)
	let free = page.pextrakeyoffset - page.pnodeoffset - 2 in
	let extrakeysize = if (extrakeylen>0) then extrakeylen+2 else 0 in
	(nodesize+extrakeysize <= free)

let zero_word = zeroes_bitstring 16

(** unsigned word bitstring of int *)
let word_to_bits word = BITSTRING {
    word : 16 : unsigned, littleendian
}

(** add new extrakey to page, push bytes into page data,
   and return new extrakey record *)
let add_extrakey page extrakeybits =
	let extralen = ((bitstring_length extrakeybits)+7)/8 in
	let len = extralen + 2 in
	let lenoffset = page.pextrakeyoffset in
  let keyoffset = lenoffset-extralen in
	(* will this pad out extrakey? *)
	let lenbits = word_to_bits len in
	bitstring_write lenbits lenoffset page.data;
  bitstring_write extrakeybits keyoffset page.data;
	(* ensure zero word at end *)
	bitstring_write zero_word (keyoffset-2) page.data;
  { koffset = lenoffset; ksize = len; extra = extrakeybits }

(** add new node record with already written optional extrakey to page,
  updating data and returning updated page. Non-blocking, should have already
	checked page has space *)
let add_node page node nodebits extrakey =
	(* update extrakey offset *)
	let pextrakeyoffset = match extrakey with
		| Some { koffset = offset; ksize = len; extra = extrakey } -> offset-len
		| None -> page.pextrakeyoffset in
	let offset = page.pnodeoffset in
	let nodesize = node.header.size in
	let pnodeoffset = page.pnodeoffset+nodesize in
	let nodes = node :: page.nodes in
	let extrakeys = match extrakey with
    | Some extrakey -> extrakey :: page.extrakeys
    | None -> page.extrakeys in
  bitstring_write nodebits offset page.data;
	(* ensure 0:16 following node *)
	bitstring_write zero_word (offset+nodesize) page.data;
	(* new page record *)
	{  page = page.page;
	   pageheader = page.pageheader;
     data = page.data; 
     nodes = nodes; 
     extrakeys = extrakeys;
     pnodeoffset = pnodeoffset; 
     pextrakeyoffset = pextrakeyoffset }

(** add an entrynode to specified page (which has enough space),
    updating page(s) as appropriate, returning new node record and updated page record *)
(*   ekeyprefix : bitstring;
  ekeyoffset : int;
  valuepage : int;
  valueoffset : int;
  timestamp : int32;
  sequence : int64;
*)
let add_entrynode page key valuepage valueoffset timestamp sequence =
	let (ekeyprefix,restofkey) = split_key key in
  let extrakey = match restofkey with
		| Some bits -> Some (add_extrakey page bits) (* NB added extrakey *)
		| None -> None in
  let ekeyoffset = match extrakey with
		| Some { koffset = lenoffset } -> lenoffset
		| None -> 0 in
	let entrynode = {
		ekeyprefix = ekeyprefix;
		ekeyoffset = ekeyoffset;
		valuepage = valuepage;
		valueoffset = valueoffset;
		timestamp = timestamp;
		sequence = sequence
	} in
	let node = { 
		noffset = page.pnodeoffset;
	  header = { size = entrynodesize; nodetype = entrynodetype };
	  body = Entrynode entrynode 
	} in
	let bits = bitstring_of_entrynode entrynode in
  let page = add_node page node bits extrakey in
	(node,page)

(** record identifying a state of the store, against which entries can be
    found, read, added *)
type version = { 
	vrootpage : int; 
	vrootoffset : int; 
	vtimestamp : int32; 
	vsequence : int64 
}

(** page/cache manager class type *)
(* what does it do?*)
(* 1. manage free space, e.g. allocate new value pages, find space for writing*)
(* nodes.*)
(* 2. cache pages in memory to reduce disk reads.*)
(* 3. schedule/manage writing pages back to disk to increase parallelism and/or*)
(* increase performance at the expense of some reduction in durability.*)
(* 4. manage updates to the root node, for consistency.*)
type session = <
  (** get version of session *)
  get_version : unit -> version; 
  (** read a node (page,offset) *)
  read_node : int -> int -> node Lwt.t;
  (** read a value page [release??] *)
  read_value_page : int -> page Lwt.t;
  (** release a value page from read_value_page *)
  release_value_page : page -> unit;
  (** get a value output page *)
  new_value_page : unit -> page Lwt.t;
  (** write a value output page *)
  write_value_page : page -> unit Lwt.t;
  (** write a node (nodesize,extrakey option,fn extrakeyoffset->node body) -> (pagenum,node) *)
  add_node : int -> bitstring option -> (int -> nodebody) -> (int * node) Lwt.t;
  (** add a new entry to the master index/version (node must be entry node), already 
      added. Blocks until (at least) this addition is in working version. *)
  add_entry : node -> unit Lwt.t;
	(** commit/release session *)
  commit : unit -> unit Lwt.t;
	(** abort *)
  abort : unit -> unit Lwt.t
>

type pagemanager = <
  (** get a session within which to interact with the database - read and/or add *)
  get_session : unit -> session
>

type pagemanagerinternal = <
  get_version : unit -> version; 
>

module IntSet = Set.Make(struct type t = int let compare = Pervasives.compare end)

type noderef = int * int
let comparenoderef ((ap,ao):noderef) ((bp,bo):noderef) =
	let cp = Pervasives.compare ap bp in
	if (cp==0) then
		Pervasives.compare ao bo
	else
		cp

module NoderefSet = Set.Make(struct type t = noderef let compare = comparenoderef end)

let cachesize = 100
type pagestatus = PageNew | PageClean | PageDirty | PageWriting
type pageinfo = { pagestatus : pagestatus; pageinfopage : page } (* more to follow? *)
 
(** page manager mutable state includes:
    list of current sessions
    cache of pages with states, including
		  new, dirty, writing, clean
	  Not sure if read_value_pages actually need tracking. *)
class sessionimpl (mgr : pagemanagerimpl) =
	object
	  val mgr = mgr;
		(** nodes read or written *)
    val mutable noderefs = NoderefSet.empty 
    (** value pages read and not released or new/written *)
    val mutable value_pages = IntSet.empty
    method get_version = mgr#get_version
    (** read a node (page,offset) *)
    method read_node (pagenum:int) (offset:int) : node Lwt.t = raise (Failure "unimplemented")
    (** read a value page [release??] *)
    method read_value_page (pagenum:int) : page Lwt.t = raise (Failure "unimplemented")
    (** release a value page from read_value_page *)
    method release_value_page (page:page) : unit = () (*noop*)
    (** get a value output page *)
    method new_value_page () : page Lwt.t = raise (Failure "unimplemented")
    (** write a value output page *)
    method write_value_page (page:page) : unit Lwt.t = raise (Failure "unimplemented")
    (** write a node (nodesize,extrakey option,fn extrakeyoffset->node body) -> (pagenum,node) *)
    method add_node (nodesize:int) (extrakey:bitstring option) (makenodebody:(int -> nodebody)) : (int * node) Lwt.t =
			raise (Failure "unimplemented")
    (** add a new entry to the master index/version (node must be entry node), already 
      added. Blocks until (at least) this addition is in working version. *)
    method add_entry (node:node) : unit Lwt.t = raise (Failure "unimplemented")
    (** commit/release session *)
    method commit () : unit Lwt.t = raise (Failure "unimplemented")
    (** abort *)
    method abort () : unit Lwt.t = raise (Failure "unimplemented")
	end 
and pagemanagerimpl (blkep : Blkifendpoint.t) =
	object(mgr)
	  val blkep = blkep;
		val mutable sessions : sessionimpl list = []
		(** FIFO list of pages in cache *)
    val cachepagenums = Array.make cachesize (-1)
		(** hashtable of pages in cache *)
    val cachepages : (int, pageinfo) Hashtbl.t = Hashtbl.create cachesize
		(** next slot in cache to use*)
    val mutable nextslot = 0
		(** cache is full? *)
    method private free_cache_slot () =
			let rec check_slot slot count =
				if count<=0 then None
				else if (cachepagenums.(slot)<0) then Some slot
				else begin
					let nextslot = if (slot+1>=cachesize) then 0 else slot+1 in
					check_slot nextslot (count-1)
				end
	   	in			
			let slot = nextslot in
			nextslot <- nextslot+1;
			check_slot nextslot cachesize
	  (** return a free cache slot, by ensuring a page is ejected *)
		method private eject_a_page () : int Lwt.t =
      (* TODO: create a task *)
			(* TODO: add the task to the page cache eject list *)
			(* TODO: wait... *)
			raise (Failure "unimplemented")			
		(** load a new page from disk into cache, return pageinfo *)
    method private load_page pagenum : page Lwt.t = 
			(* TODO: eject a page from the cache if necessary *)
			lwt freeslot = match (free_cache_slot ()) with
				| Some slot -> return slot
				| None ->
					eject_a_page ()
			in
			(* TODO: create a task *)
			(* TODO: add the page/task to the page load list *)
			(* TODO: wait... *)
		  raise (Failure "unimplemented")         

		(** get page in cache, load if necessary *)
    method get_page pagenum =
			lwt pip = try 
				return (Hashtbl.find cachepages pagenum)
			with Not_found -> 
				load_page pagenum
			in
			pip.pageinfopage
    method get_version () = { 
			(* TODO *)
			vrootpage = 0; vrootoffset = 0; vtimestamp = 0l; vsequence = 0L 
	  } 
		method get_session () : session = 
			let session = new sessionimpl ( mgr :> pagemanagerimpl ) in
			sessions <- session :: sessions;
			session
	end
	

(** test - needs Blkif 'test' *)
let test1 () =
  OS.Console.log "Looking for block device 'test'...";
  (* get/open the "test" block device as a block endpoint *)
  lwt blkep = Blkifendpoint.get "test" in 
  OS.Console.log (sprintf "OK, got blkep name %s" blkep#id);
	(* read block 0 *)
  lwt (indata,inbuf) = match_lwt (blkep#blkin#recv 0) with
      | Data (data,(),buf) -> return (data,buf)
      | _ -> OS.Console.log (sprintf "Error reading block"); 
        raise_lwt ( Failure "reading block" ) in
	(* parse *)
  let rootpage = parse_page 0 indata in
	let get_extrakey page offset =
		if (offset=0) then empty_bitstring
		else 
			let rec find_extrakey extrakeys offset = match extrakeys with
				| [] -> empty_bitstring
				| { koffset = koffset ; extra = extra } :: tl -> 
					if (koffset=offset) then extra
					else find_extrakey tl offset in
			find_extrakey page.extrakeys offset
	in
	(* print nodes *)
	let print page n  = match n with
        | { noffset = noffset; body = Rootnode { 
              rootpage=rootpage; rootoffset=rootoffset; lastpage=lastpage } } ->
                OS.Console.log (sprintf "Root node at %d is %d:%d, last page is %d" 
          noffset rootpage rootoffset lastpage)
        | { noffset = noffset; body = Entrynode { 
              ekeyprefix=ekeyprefix; ekeyoffset=ekeyoffset; valuepage=valuepage;
							valueoffset=valueoffset; timestamp=timestamp; sequence=sequence } } ->
								let keyprefix = string_of_bitstring ekeyprefix in
								let keyprefix =
									try
										let ix = String.index keyprefix (Char.chr 0) in
										String.sub keyprefix 0 ix
									with Not_found -> keyprefix in
								let extrakey = string_of_bitstring (get_extrakey page ekeyoffset) in
                OS.Console.log (sprintf "Entry node at %d with key %s+@%d:%s, value at %d:%d, timestamp %ld, sequence %Ld" 
                noffset keyprefix ekeyoffset extrakey valuepage valueoffset timestamp sequence )
        | { noffset = noffset; header = { nodetype = nodetype } } ->
               OS.Console.log (sprintf "Node %d at %d" nodetype noffset) in
	List.iter (print rootpage) rootpage.nodes ;
	(* TODO: create and add an entry node, initially no extra key *)
	let key = bitstring_of_string "abcdefghijklmnopq" in
	let extrakeylen = extrakey_len key in
	(* TODO: enough space in this page? if not allocate a new free page (update last) *)
	let ok = is_room_for_node rootpage entrynodesize extrakeylen in
 	if (ok) then begin
    let (node,page) = add_entrynode rootpage key 0 0 (Int32.of_float (OS.Clock.time ())) 1L in
	  OS.Console.log (sprintf "added entry node at %d" node.noffset);
		write_page blkep page
 	end else begin
	  	OS.Console.log "No room for new node on page";
			return ()
	end >> 
	(* TODO: create entrynode record; convert to bitstring; copy into current free*)
	(* space in block; schedule write back; update cache *)
	return ()
