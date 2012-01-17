(* Endpoint abstraction for a simple (no-sql) datastore. That's the idea, 
 * anyway... 
 * 
 * Next:
 * 2) define value nodes and un/marhshalling routines; implement allocation for
 * new and link into entry node; implement bytestreamout on top of this. 
 * Implement bytestreamin on top of read value pages.
 * 3) limit size of pagecache in memory (i.e. add eject!)
 * 
 * One day) sort out handling concurrent addition of entities in btree gen/
 * root update
 * One day) implement garbage collection
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
 *  256- = value-nodes of varying levels (level 256 refers to value whole-pages (offset 0))
 *  2 = entry node
 *  512- = b-tree-nodes of varying levels
 * 
 * Page ID:
 *  1 = structured page, v.1
 * 
 * value-node = BITSTRING {
 *   size [16] : 16 : unsigned;
 *   nodetype [256-511] : 16 : unsigned; 
 *   v0pad : 2;
 *   v0page : 30 : unsigned;
 *   v0offset : 16;
 *   v1pad : 2;
 *   v1page : 30 : unsigned;
 *   v1offset : 16;
 *   v2pad : 2;
 *   v2page : 30 : unsigned;
 *   v2offset : 16;
 *   v3pad : 2;
 *   v3page : 30 : unsigned;
 *   v3offset : 16;
 * }
 * Total = 28
 * 
 * entry-node = BITSTRING {
 *   size [32] : 16 : unsigned;
 *   nodetype [2] : 16 : unsigned; 
 *   keyprefix : 64 : bitstring;
 *   keyoffset : 16 : unsigned;
 *   0 : 2;
 *   valuepage : 30 : unsigned;
 *   valueoffset : 16 : unsigned;
 *   valuelength : 64; 
 *   timestamp : 32;
 *   sequence : 64;
 * }
 * Total = 40
 * 
 * btree-node = BITSTRING {
 *   size [26] : 16 : unsigned;
 *   nodetype [512-] : 16 : unsigned;
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
 *   lastnodepage : 30 : unsigned; (* last node page in use *)
 *   reserved...
 * }
 * Total = 24+reserved = 32
 * 
 * Chris Greenhalgh, University of Nottingham, 2012-01-04.
 *)

open Endpoint
open Lwt
open Printf
open Bitstring

(*---------------------------------------------------------------------------*)
(* Abstract simpleDB interface *)

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

(*---------------------------------------------------------------------------*)
(* constants and sizes for on-disk storage *)

(** page header size, bytes *)
let pageheadersize = 4
(** root page type *)
let freepagetype = 0
let rootpagetype = 1
let nodepagetype = 2
let valuepagetype = 3
    
type pagetype = Rootpagetype | Nodepagetype | Valuepagetype | Freepagetype
		
(** root node size *)
let rootnodesize = 32
(** root node type *)
let rootnodetype = 1
(** magic *)
let magic = 0x513913DBl
(** version *)
let version = 1

(** entry node type *)
let entrynodetype = 2

(** bits of key in prefix (bytes) *)
let keyprefixlength = 8
(** length of entrynode record *)
let entrynodesize = 40

(** btree node type *)
let btreenodetype = 512
(** btree node type mask *)
let btreenodetypemask = 0xff00
let btreenodelevelmask = 0x00ff

(** btree node size *)
let btreenodesize = 26

(** value node size *)
let valuenodesize = 28
let valuenodetype = 256
let valuenodetypemask = 0xff00
let valuenodelevelmask = 0x00ff

(*---------------------------------------------------------------------------*)
(* in-memory structures for underlying data storage *)

(** page header record *)
type pageheader = {
	pagetype : int;
	pagesizelog2 : int
}

type nodeheader = {
	size : int;
	nodetype : int
}

(** root node record *)
type rootnodebody = {
	(* after common header *)
  magic : int32;
  version : int;
  rootpage : int;
  rootoffset : int;
  lastpage: int;
  lastnodepage: int;
}

type rootnode = {
	rnpage : int;
	rnoffset : int;
	rheader : nodeheader;
	rbody : rootnodebody
}

(** entry node record *)
type entrynodebody = {
  (* after common header *)
  ekeyprefix : bitstring;
  ekeyoffset : int;
	(* actually in extrakey section *)
	ekeyextra : bitstring option;
  valuepage : int;
  valueoffset : int;
	valuelength : int64;
  timestamp : int32;
  sequence : int64;
}

type entrynode = {
	enpage : int;
	enoffset : int;
	eheader : nodeheader;
	ebody : entrynodebody
}

(** btree node record *)
type btreenodebody = {
	(* encoded into type *)
	level : int;
  bkeyprefix : bitstring;
  bkeyoffset : int;
	(* actually in extrakey section *)
	bkeyextra : bitstring option;
  lpage : int;
  loffset : int;
  rpage : int;
  roffset : int;
}

type btreenode = {
	bnpage : int;
	bnoffset : int;
	bheader : nodeheader;
	bbody : btreenodebody
}

(** value node record *)
type valuenodebody = {
	vlevel : int;
  v0page : int;
 	v0offset : int;
  v1page : int;
  v1offset : int;
  v2page : int;
  v2offset : int;
  v3page : int;
  v3offset : int;
}

type valuenode = {
	vnpage : int;
	vnoffset : int;
	vheader : nodeheader;
	vbody : valuenodebody
}

type nodebody = Rootnodebody of rootnodebody
 | Entrynodebody of entrynodebody
 | Btreenodebody of btreenodebody
 | Valuenodebody of valuenodebody
 | Unknownnodebody

type node = Rootnode of rootnode
 | Entrynode of entrynode
 | Btreenode of btreenode
 | Valuenode of valuenode
 | Unknownnode

(** get offset *)
let get_node_offset node : int = match node with
	| Entrynode { enoffset } -> enoffset
	| Rootnode { rnoffset } -> rnoffset
	| Btreenode { bnoffset } -> bnoffset
	| Valuenode { vnoffset } -> vnoffset
	| Unknownnode -> raise (Failure "get_node_offset for Unknownnode")

(** get page *)
let get_node_page node : int = match node with
	| Entrynode { enpage } -> enpage
	| Rootnode { rnpage } -> rnpage
	| Btreenode { bnpage } -> bnpage
	| Valuenode { vnpage } -> vnpage
	| Unknownnode -> raise (Failure "get_node_page for Unknownnode")

(** get page/offset *)
let get_node_ref node : (int*int)= match node with
	| Entrynode { enpage; enoffset } -> (enpage,enoffset)
	| Rootnode { rnpage; rnoffset } -> (rnpage,rnoffset)
	| Btreenode { bnpage; bnoffset  } -> (bnpage,bnoffset)
	| Valuenode { vnpage; vnoffset  } -> (vnpage,vnoffset)
	| Unknownnode -> raise (Failure "get_node_ref for Unknownnode")

(** get nodeheader *)
let get_node_header node : nodeheader = match node with
	| Entrynode { eheader } -> eheader
	| Rootnode { rheader } -> rheader
	| Btreenode { bheader } -> bheader
	| Valuenode { vheader } -> vheader
	| Unknownnode -> raise (Failure "get_node_header for Unknownnode")
 
let get_node_level node : int = match node with
	| Entrynode _ -> 0
	| Btreenode { bbody = { level } } -> level
	| Valuenode { vbody = { vlevel } } -> vlevel
	(* incomplete *)

type keyvalue = bitstring * (bitstring option)

let zero_keyvalue = (zeroes_bitstring (keyprefixlength*8), None)

let get_keyvalue node : keyvalue = match node with
	| Entrynode { ebody = { ekeyprefix; ekeyextra } } -> (ekeyprefix,ekeyextra)
	| Btreenode { bbody = { bkeyprefix; bkeyextra } } -> (bkeyprefix,bkeyextra)
	| _ -> zero_keyvalue

(** info about extra key info at end of page *)
type extrakey = {
	(** offset is page offset of the size word, at the end of the extrakey *)
	koffset : int;
	(** bytes *)
	ksize : int;
	extra : bitstring
}

let int_of_pagetype pagetype = match pagetype with
	| Rootpagetype -> rootpagetype
	| Nodepagetype -> nodepagetype
	| Valuepagetype -> valuepagetype
  | Freepagetype -> freepagetype
	
let string_of_pagetype pagetype = match pagetype with
	| Rootpagetype -> "Rootpagetype"
	| Nodepagetype -> "Nodepagetype"
	| Valuepagetype -> "Valuepagetype"
  | Freepagetype -> "Freepagetype"
	
let pagetype_of_int pti = 
	if (pti=rootpagetype) then Rootpagetype
	else if (pti=nodepagetype) then Nodepagetype
	else if (pti=valuepagetype) then Valuepagetype
	else if (pti=freepagetype) then Freepagetype
	else raise (Failure (sprintf "Unknown page type %d" pti))

(** page cache entry *)
type page = {
	(** which page *)
  page : int;
	(** page header *)
  pageheader : pageheader;
	(** page data *)
  data : bitstring;
	(** the root node entry *)
  rootnode : rootnode option;
	(** the (other) nodes - bottom up*)
  nodes : node list;
	(** the extra keys - top down *)
	extrakeys : extrakey list;
	(** offset of 'end' (type 0) node found *)
	pnodeoffset : int;
	(** offset of 'end' (size 0) extrakey found *)
  pextrakeyoffset : int
}

(*---------------------------------------------------------------------------*)
(* helper routines for marshalling/unmarshalling between on disk and in-memory*)
(* structures *)

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

(** read extrakey value option (when parsing node) *)
let parse_extrakey pagedata keyoffset : bitstring option = match keyoffset with
	| 0 -> None
	| _ -> begin
		if ((keyoffset >= pageheadersize) &&  (keyoffset*8+16 <= bitstring_length pagedata)) then 
			bitmatch (dropbits (keyoffset*8) pagedata) with
		  {
				keylen : 16 : unsigned, littleendian
		  } -> begin
				if (keylen=0) then None 
				else if (keyoffset+2-keylen<0) then (
  				OS.Console.log (sprintf "extrakey length out of range (%d@%d)" keylen keyoffset);				
					raise (Failure "extrakey length out of range")
				) else 
					Some (subbitstring pagedata ((keyoffset+2-keylen)*8) ((keylen-2)*8))
			end
	  else begin 
			OS.Console.log (sprintf "extrakey offset out of range (%d)" keyoffset);		
			raise (Failure "extrakey offset out of range")
	  end
	end
	
(** try to parse a node - any type. data starts at current page *)
let parse_node pagedata offset size nodetype pagenum : node =
	(* skip header *)
	let data = subbitstring pagedata ((offset+4)*8) (8*(size-4)) in
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
      lastpage : 30 : unsigned, littleendian;
      0 : 2; (* pad *)
      lastnodepage : 30 : unsigned, littleendian;        
      0 : 2 (* pad *)
     } -> Rootnode { rnpage = pagenum; rnoffset = offset; rheader = header;
		        rbody = { 
            magic = magic; 
            version = version;
            rootpage = rootpage;
            rootoffset = rootoffset;
            lastpage = lastpage;
						lastnodepage = lastnodepage
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
			valuelength : 64 : littleendian;
      timestamp : 32 : littleendian;
      sequence : 64 : littleendian
    } -> Entrynode { enpage = pagenum; enoffset = offset; eheader = header;
           ebody = { 
           ekeyprefix = ekeyprefix; 
           ekeyoffset = ekeyoffset;
					 ekeyextra = parse_extrakey pagedata ekeyoffset;
				   valuepage = valuepage;
					 valueoffset = valueoffset;
					 valuelength;
					 timestamp = timestamp;
					 sequence = sequence
         } }
  else if ((nodetype land btreenodetypemask) = btreenodetype) then begin
		let level = nodetype land btreenodelevelmask in
		bitmatch data with {
      bkeyprefix : keyprefixlength*8 : bitstring;
      bkeyoffset : 16 : unsigned, littleendian;
      0 : 2;
      lpage : 30 : unsigned, littleendian;
      loffset : 16: unsigned, littleendian;
      0 : 2;
      rpage : 30 : unsigned, littleendian;
      roffset : 16: unsigned, littleendian
    } -> Btreenode { 
			bnpage = pagenum; bnoffset = offset; bheader = header;
      bbody = {
		  	level = level;
    		bkeyprefix = bkeyprefix;
			  bkeyoffset = bkeyoffset;
				bkeyextra = parse_extrakey pagedata bkeyoffset;
			  lpage = lpage;
			  loffset = loffset;
			  rpage = rpage;
			  roffset = roffset;
		} }
  end else if ((nodetype land valuenodetypemask) = valuenodetype) then begin
		let vlevel = nodetype land valuenodelevelmask in
		bitmatch data with {
	    0 : 2;
  	  v0page : 30 : unsigned, littleendian;
    	v0offset : 16 : unsigned, littleendian;
    	0 : 2;
    	v1page : 30 : unsigned, littleendian;
    	v1offset : 16 : unsigned, littleendian;
    	0 : 2;
    	v2page : 30 : unsigned, littleendian;
    	v2offset : 16 : unsigned, littleendian;
    	0 : 2;
    	v3page : 30 : unsigned, littleendian;
    	v3offset : 16 : unsigned, littleendian
    } -> Valuenode { 
			vnpage = pagenum; vnoffset = offset; vheader = header;
      vbody = { vlevel; v0page; v0offset; v1page; v1offset; v2page; v2offset; v3page; v3offset }
		} 
	end else
		raise (Failure (sprintf "Unknown node type %d" nodetype))	
				
(** create a rootnode for given root page & offset *)
let create_rootnodebody rootpage rootoffset lastpage lastnodepage = {
		magic; version; rootpage; rootoffset; lastpage; lastnodepage }
	
let bitstring_of_rootnodebody n = BITSTRING {
	  rootnodesize : 16 : unsigned, littleendian;
		rootnodetype : 16 : unsigned, littleendian;
		n.magic : 32 : bigendian; (* magic *)
		n.version : 16 : littleendian; (* version *)
		n.rootpage : 30 : unsigned, littleendian;
    0 : 2; (* pad *)
		n.rootoffset : 16 : unsigned, littleendian;
		n.lastpage : 30 : unsigned, littleendian; (* last page *)
    0 : 2 (* pad *);
    n.lastnodepage : 30 : unsigned, littleendian; (* last nodepage *)
    0 : 2 (* pad *)
		(* reserved *)
	}
	
let bitstring_of_entrynodebody n = 
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
		n.valuelength : 64 : littleendian;
    n.timestamp : 32 : littleendian;
    n.sequence : 64 : littleendian
	}

let bitstring_of_btreenodebody b =
	BITSTRING {
		btreenodesize : 16 : unsigned, littleendian;
		(btreenodetype+b.level) : 16 : unsigned, littleendian;
    b.bkeyprefix : keyprefixlength*8 : bitstring;
    b.bkeyoffset : 16 : unsigned, littleendian;
    0 : 2;
    b.lpage : 30 : unsigned, littleendian;
    b.loffset : 16: unsigned, littleendian;
    0 : 2;
    b.rpage : 30 : unsigned, littleendian;
    b.roffset : 16: unsigned, littleendian
	}

let bitstring_of_valuenodebody b =
	BITSTRING {
    valuenodesize : 16 : unsigned, littleendian;
    (valuenodetype+b.vlevel) : 16 : unsigned, littleendian; 
    0 : 2;
    b.v0page : 30 : unsigned, littleendian;
    b.v0offset : 16 : unsigned, littleendian;
    0 : 2;
    b.v1page : 30 : unsigned, littleendian;
    b.v1offset : 16 : unsigned, littleendian;
    0 : 2;
    b.v2page : 30 : unsigned, littleendian;
    b.v2offset : 16 : unsigned, littleendian;
    0 : 2;
    b.v3page : 30 : unsigned, littleendian;
    b.v3offset : 16 : unsigned, littleendian
  }

let bitstring_of_node node = match node with 
	| Entrynode { ebody } ->	bitstring_of_entrynodebody ebody
  | Btreenode{ bbody } -> bitstring_of_btreenodebody bbody
	| Rootnode {rbody} -> bitstring_of_rootnodebody rbody
	| Valuenode { vbody } -> bitstring_of_valuenodebody vbody
  | Unknownnode _ -> raise (Failure "bitstring_of_node does not support Unknownnode")

let sizelog2 size = 
    let rec div2 size count = match size with
        | 0 -> count
        | s -> div2 (s/2) count+1
    in (div2 size (-1))

	
(*---------------------------------------------------------------------------*)
(* bootstrap stuff to initialise a blank database file *)

(** ininitialise a root page bitstring with a page header and root node, only *)
let create_rootpage size sizelog2 = 
	let rootheader = bitstring_of_pageheader 
  	{ pagetype=rootpagetype; pagesizelog2=sizelog2 } in 
	let rootnodebody = create_rootnodebody 0 0 0 0 in
	let rootnodebits = bitstring_of_rootnodebody rootnodebody in
	concat [ rootheader; rootnodebits; zeroes_bitstring (((size-pageheadersize)*8)-bitstring_length rootnodebits) ]

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

(*---------------------------------------------------------------------------*)
(* higher-level page parsing and checking helpers *)

let get_root_node nodes =
  let rec get_root nodes =
    match nodes with 
      | [] -> None
      | n :: ns -> begin
        match n with
          | Rootnode rn -> Some rn
          | _ -> get_root ns
        end
   in get_root nodes

let rec omit_root_node nodes =
    match nodes with
        | [] -> []
        | n :: ns -> begin
            match n with
                | Rootnode _ -> omit_root_node ns
                | _ -> n::(omit_root_node ns)
            end

(** try to parse a structured page *)
let parse_page pagenum data : page =
    let pageheader = parse_pageheader data in
		let hasnodes = pageheader.pagetype=rootpagetype || pageheader.pagetype=nodepagetype in
		if (not hasnodes) then
	    { page = pagenum; pageheader = pageheader;
	      data = data; rootnode = None; nodes = []; pnodeoffset = 0;
      	extrakeys = []; pextrakeyoffset = 0 }
		else (
    (* data starts at current page *)
    let rec read_nodes pagedata offset nodes = bitmatch (dropbits (offset*8) pagedata) with
        { size : 16 : unsigned, littleendian;
            nodetype : 16 : unsigned, littleendian
      } -> 
            if ((offset+size)*8 > bitstring_length data) then
          raise (Failure (sprintf "node size too big (%d/%d)" (bitstring_length data) (size*8)))
      else if (size==0) then
              (* end *) (nodes,offset)
            else begin
                let node = parse_node pagedata offset size nodetype pagenum in
                let nodes = node :: nodes in
                read_nodes pagedata (offset+size) nodes
            end
    in
    (* parse nodes *)
    let (nodes,pnodeoffset) = read_nodes data pageheadersize [] in
    let rootnode = get_root_node nodes in
    let nodes = omit_root_node nodes in 
    (* parse extra keys - don't really need the full info now as key is read directly when*)
		(* parsing node, but need pextrakeyoffset (end of list) and could be useful for checking *)
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
    { page = pagenum; pageheader = pageheader;
      data = data; rootnode = rootnode; nodes = nodes; pnodeoffset = pnodeoffset;
      extrakeys = extrakeys; pextrakeyoffset = pextrakeyoffset }
    )    
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
	let nodesize = (get_node_header node).size in
	let pnodeoffset = page.pnodeoffset+nodesize in
	let nodes = node :: page.nodes in
	let extrakeys = match extrakey with
    | Some extrakey -> extrakey :: page.extrakeys
    | None -> page.extrakeys in
  bitstring_write nodebits offset page.data;
	(* ensure 0:16 following node *)
	bitstring_write zero_word (offset+nodesize) page.data;
	(* new page record *)
	{  page with 
     nodes = nodes; 
     extrakeys = extrakeys;
     pnodeoffset = pnodeoffset; 
     pextrakeyoffset = pextrakeyoffset }

let compare_keyvalue (kvp1,kve1) (kvp2,kve1) : int =
	let prefixcmp = Bitstring.compare kvp1 kvp2 in
	if (prefixcmp != 0) then prefixcmp 
	else match (kve1,kve1) with
		| (None,None) -> 0
		| (Some bs,None) -> Bitstring.compare bs (zeroes_bitstring (bitstring_length bs))
		| (None,Some bs) -> Bitstring.compare (zeroes_bitstring (bitstring_length bs)) bs
		| (Some bs1,Some bs2) -> Bitstring.compare bs1 bs2

(** compare two entrynodes by key *)
let compare_entrynode en1 en2 : int =
	compare_keyvalue (en1.ebody.ekeyprefix,en1.ebody.ekeyextra) (en2.ebody.ekeyprefix,en2.ebody.ekeyextra)		

let dump_of_keyvalue (ekeyprefix,ekeyextra) = 
	let keyprefix = string_of_bitstring ekeyprefix in
	let keyprefix =
		try
			let ix = String.index keyprefix (Char.chr 0) in
			String.sub keyprefix 0 ix
		with Not_found -> keyprefix in
	let extrakey = match ekeyextra with
		| None -> ""
		| Some bs -> string_of_bitstring bs 
  in (sprintf "%s+%s" keyprefix extrakey)

(*---------------------------------------------------------------------------*)
(* in-memory page cache with actual reads/write passed off to *)
(* background/helper threads (could be scheduled, etc.).*)
(* Currently has dependencies on the specific in-memory page structure, so*)
(* is specific to this application, but this should be easily generalisable. *)

(** page cache... *)

let cachesize = 100
type pagestatus = 
	| PageRequested (* asked for *)
	| PageReading (* in process *)
	| PageNew (* new not written *)
	| PageClean 
	| PageDirty 
	| PageWriting 
	| PageWritingDirty
	
type pageinfo = { 
	pageinfonum : int;
	pagestatus : pagestatus ref; 
	pageinfopage : page option ref; 
	pagelock : int ref;
	waitingforread : page Lwt.u list ref;
} (* more to follow? *)


type pagecache = <
  (** page size *)
  page_size : int;
  (** lock page for read or write *)
  lock_page : int -> page Lwt.t;
	(** release lock on page (pagenum,dirty,new page option) *)
	release_page : int -> bool -> page option -> unit;
	(* wait for write? *) 
	(** new_page data -> pagenum *)
	(*new_page : bitstring -> int Lwt.t;*)
>

(** infinite FIFO with blocking get, passing page numbers as job hints, 
    from pagecache users to pagecache workers, to allow behind-the-scenes
    management/scheduling of actual reads/writes *)
class jobqueue =
	object (self)
	  val jobs : int Lwt_sequence.t = Lwt_sequence.create ()
	  val waiters : unit Lwt.u Lwt_sequence.t = Lwt_sequence.create ()
		method put job : unit =
			ignore (Lwt_sequence.add_l job jobs);
			try 
				let waiter = Lwt_sequence.take_r waiters in
				Lwt.wakeup waiter ()
			with Lwt_sequence.Empty -> ()
		method get () : int Lwt.t =
			try 
				let job = Lwt_sequence.take_r jobs in
				return job
			with Lwt_sequence.Empty -> begin
				let (thread,waiter) = Lwt.wait () in
				ignore (Lwt_sequence.add_l waiter waiters);
				Lwt.bind thread self#get
		  end
	end

(** page cache implementation *)
class pagecacheimpl (blkep : Blkifendpoint.blkendpoint) =
	object (self)
	  val blkep = blkep
		method page_size = blkep#block_size
		(** hashtable of pages in cache *)
    val cachepages : (int, pageinfo) Hashtbl.t = Hashtbl.create cachesize
		(** jobqueue to coordinate with worker thread(s) *)
		val jobqueue = new jobqueue
		(** start worker *)
		initializer 
    	let (workert,workeru) = Lwt.task () in
			let rec worker_task () : unit Lwt.t = 
				OS.Console.log "worker waiting for request...";
				lwt req = jobqueue#get() in
				self#do_work req >>
				worker_task () in
			ignore (bind workert worker_task);
			wakeup workeru ()
		(** worker *)
    method do_work pagenum =
				OS.Console.log (sprintf "received pagecache request %d" pagenum);
				if (Hashtbl.mem cachepages pagenum = false) then begin
					OS.Console.log (sprintf "Pageinfo not found for request %d" pagenum);
					return ()
				end else begin
					let pageinfo = Hashtbl.find cachepages pagenum in
					match !(pageinfo.pagestatus) with
						| PageRequested -> begin
							(* TODO cache size? eject? *)
							OS.Console.log (sprintf "read page %d" pagenum);
	 				  	match_lwt (blkep#blkin#recv 0) with
      					| Data (data,(),buf) -> begin
										(* ok! *)
										let page = parse_page pagenum data in
										pageinfo.pageinfopage := Some page;
										pageinfo.pagestatus := PageClean;
										let waitingforread = !(pageinfo.waitingforread) in
										pageinfo.waitingforread := [];
										List.iter (fun u -> Lwt.wakeup u page) waitingforread;
										return ()
									end
					      | _ -> begin
										OS.Console.log (sprintf "Error reading block %d" pagenum);
										(* retry *)
										jobqueue#put pagenum; 
										return ()
									end
						  end
					  | PageDirty 
						| PageWritingDirty -> begin
						  OS.Console.log (sprintf "write page %d" pagenum);
							pageinfo.pagestatus := PageWriting;
							(* not exhaustive but should be ok from state *)
							let Some page = !(pageinfo.pageinfopage) in
							lwt (outdata,(),outbuf) = blkep#blkout#getbuf pagenum in
						  bitstring_write page.data 0 outdata;
						  (* write to disk *)
  						outbuf#send () >>
  						(* happy :) *)
  						(OS.Console.log (sprintf "wrote page %d" pagenum);
				      return ())
						  end
					 |_ -> begin 
							OS.Console.log (sprintf "don't know what to do with page %d" pagenum);
							return ()
						 end
				end
    (** lock page for read or write *)
    method lock_page (pagenum:int) : page Lwt.t =
			OS.Console.log (sprintf "lock_page(%d)" pagenum);
			let page_ready_state state = match state with 
				| PageClean 
				| PageNew
				| PageDirty
				| PageWriting 
				| PageWritingDirty -> true
				| PageRequested
				| PageReading -> false
			in let page_in_cache () = (Hashtbl.mem cachepages pagenum) in
			let rec ensure_load () : page Lwt.t =
				if (page_in_cache()=false) then begin
					Hashtbl.add cachepages pagenum { 
						pageinfonum = pagenum;
						pagestatus = ref PageRequested;
						pageinfopage = ref None;
						pagelock = ref 0;
						waitingforread = ref [] };
					ensure_load ()
 				end else begin
					let pageinfo = Hashtbl.find cachepages pagenum in
					if (page_ready_state !(pageinfo.pagestatus)) then begin
						pageinfo.pagelock := 1+ !(pageinfo.pagelock);
						(* not exhaustive, but should follow from state *)
						let Some page = !(pageinfo.pageinfopage) in
						return page
					end else begin
						(* task... *)
						let t,u = Lwt.task () in
						pageinfo.waitingforread := u :: !(pageinfo.waitingforread);
						OS.Console.log (sprintf "lock_page waiting for read on %d" pagenum);
						jobqueue#put pagenum;
						choose (t :: []) >>
						ensure_load ()
					end 
				end in
			ensure_load()
  	(** release lock on page *)
	  method release_page (pagenum:int) (dirty:bool) (page:page option) : unit = 
      OS.Console.log (sprintf "release_page(%d,%s,%s)" pagenum 
			  (if dirty then "dirty" else "clean") 
				(match page with Some page -> "new page" | None -> "unchanged"));
			let pageinfo = Hashtbl.find cachepages pagenum in
			pageinfo.pagelock := !(pageinfo.pagelock) -1;
			begin 
			  match page with 
				  | Some page -> pageinfo.pageinfopage := Some page
				  | _ -> ()
			end;
			let unlocked = !(pageinfo.pagelock) = 0 in
  		let pagestatus =  !(pageinfo.pagestatus) in
			let was_clean = match pagestatus with
				| PageDirty 
				| PageWritingDirty -> false
				| _ -> true 
			in begin
			if (dirty) then begin
				pageinfo.pagestatus := (match pagestatus with
					| PageClean -> PageDirty
					| PageDirty -> PageDirty
					| PageWriting 
					| PageWritingDirty -> PageWritingDirty
					| PageRequested 
					| PageNew 
					| PageReading -> raise (Failure "release_page of page in invalid state"));
				(*OS.Console.log (sprintf "change page %d status to dirty" pagenum)*)
			end
			end;
			if (dirty && was_clean) then begin
				(* queue flush *)
				OS.Console.log (sprintf "schedule job for %d on release made dirty" pagenum);
				jobqueue#put pagenum				
			end else if (unlocked) then begin
	      (* eject? *)
        OS.Console.log (sprintf "schedule job for %d on release unlocked clean" pagenum);
	      jobqueue#put pagenum                
    	end else 
				OS.Console.log (sprintf "no schedule, dirty=%s was_clean=%s unlocked=%s"
				  (if dirty then "t" else "f") 
					(if was_clean then "t" else "f") 
          (if  unlocked then "t" else "f") )
  	(* wait for write? *) 
	  (** new_page data -> pagenum. Or could be lock_without_read *)
	  (*method new_page (bits:bitstring) : int Lwt.t = raise (Failure "unimplemented")*)
	end

let testcache() = 
  OS.Console.log "Looking for block device 'test'...";
  (* get/open the "test" block device as a block endpoint *)
  lwt blkep = Blkifendpoint.get "test" in 
  OS.Console.log (sprintf "OK, got blkep name %s" blkep#id);
  let blockcache : pagecache = ((new pagecacheimpl blkep) :> pagecache) in
	lwt page = blockcache#lock_page 0 in
	OS.Console.log "Got page 0!"; 
	blockcache#release_page 0 false None;
	OS.Console.log "Released page 0!"; 
	lwt page = blockcache#lock_page 0 in
	OS.Console.log "Got page 0!"; 
	blockcache#release_page 0 true None;
	OS.Console.log "Released page 0 (dirty)!"; 
	return ()	

(*---------------------------------------------------------------------------*)
(* internal persistence interface operating at the level of disk nodes and*)
(* blocks, which can then be used to implemented abstract service *)

(** record identifying a state of the store, against which entries can be
    found, read, added *)
type version = { 
	vrootpage : int; 
	vrootoffset : int; 
	vtimestamp : int32; 
	vsequence : int64 
}

(** internal persistence interface for simpledb, i.e. above page level *)
(* what does it do?*)
(* 0. node/value page API abstraction over low-level block store/cache *)
(* 1. manage free space, e.g. allocate new value pages, find space for writing*)
(* nodes.*)
(* 2. manage updates to the root node, for consistency.*)
(* 3. keep hooks allowing GC to be added later *)
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
  (** write a node (nodesize,nodetype,extrakey option,fn extrakeyoffset->node body) -> (page,node) *)
  add_node : int -> int -> bitstring option -> (int -> nodebody) -> node Lwt.t;
  (** add a new entry to the master index/version (node must be entry node), already 
      added. Blocks until (at least) this addition is in working version. *)
  add_entry : entrynode -> unit Lwt.t;
	(** commit/release session *)
  commit : unit -> unit Lwt.t;
	(** abort *)
  abort : unit -> unit Lwt.t
>

type sessionmanager = <
  (** get a session within which to interact with the database - read and/or add *)
  get_session : unit -> session Lwt.t
>

(*---------------------------------------------------------------------------*)
(* implementation of the internal persistence layer (nodes & value blocks) *)

(* ordered set of Int *)
module IntSet = Set.Make(struct type t = int let compare = Pervasives.compare end)

type noderef = int * int
let comparenoderef ((ap,ao):noderef) ((bp,bo):noderef) =
	let cp = Pervasives.compare ap bp in
	if (cp==0) then
		Pervasives.compare ao bo
	else
		cp

(* ordered set of Noderef, i.e. page+offset *)
module NoderefSet = Set.Make(struct type t = noderef let compare = comparenoderef end)

let is_null_ref (page,offset) = page=0 && offset=0

(* Btree iterator helper *)
type btreeiterstate = Left | Right
type btreeiterrec = { btinode : btreenode; btistate : btreeiterstate }

type btreeitert = (entrynode option * btreeiterrec list)

let bti_entrynode t = match t with
	| (Some en, _) -> Some en
	| (None, _) -> None

let rec bti_go_left (getnode:(noderef -> node Lwt.t)) (page,offset) (eno,recs) : btreeitert Lwt.t =
  if (is_null_ref (page,offset)) then return (eno,recs) 
  else begin
    lwt node = getnode (page,offset) in
		match node with 
    	| Entrynode en -> return (Some en,recs)                  
      | Btreenode bn -> 
				let recs = { btinode=bn; btistate = Left } :: recs in
        bti_go_left getnode (bn.bbody.lpage,bn.bbody.loffset) (eno,recs)
      | _ -> begin
          OS.Console.log (sprintf "invalid node type found at %d/%d in btree" page offset);
					return (eno,recs)
       end
	end

let bti_advance (getnode:(noderef -> node Lwt.t)) (eno,recs) : btreeitert Lwt.t =
  match eno with
		| None -> return (None,[]) (* noop *)
		| Some en -> begin
		  (* pop all trues off the top *)
        let rec skip_right stack = match stack with
					| { btistate = Right} :: rest -> skip_right rest
					| _ -> stack
				in 
				let recs = skip_right recs in
				(* if there is a false left, swap to true... *)
				match recs with
					| [] -> (* done *) return (None,[])
					| { btistate = Left; btinode = bn } as btirec :: ts -> 
						let recs = { btirec with btistate = Right } :: ts in
						(* now traverse to bottom of right branch, keeping left *)
						bti_go_left getnode (bn.bbody.rpage,bn.bbody.roffset) (None,recs)
					| _ -> raise (Failure "Weird error: btreeiter stack with true left at top")
			end

let bti_new (getnode:(noderef -> node Lwt.t)) rootref : btreeitert Lwt.t = 
	bti_go_left getnode rootref (None,[])

(* a node at the top in a new index; maxvalue is right; btbbtirecs is (optional) old node parents for possible reuse *)
type btreebuildrec = { btbnode : node; btbmaxvalue : keyvalue; btblevel : int; btbbtirecs : btreeiterrec list }

(** session manager mutable state includes:
    list of current sessions
    cache of pages with states, including
		  new, dirty, writing, clean
	  Not sure if read_value_pages actually need tracking. *)
class sessionimpl (mgr : sessionmanagerimpl) (pagecache : pagecache) (version : version) =
	object (self) 
	  val mgr = mgr;
		val pagecache = pagecache;
		val version = version;
		(** nodes read or written *)
    val mutable noderefs = NoderefSet.empty 
    (** value pages read and not released *)
    val mutable read_value_pages = IntSet.empty
    (** new value pages not released written *)
    val mutable new_value_pages = IntSet.empty
    (** new value pages written not committed *)
    val mutable written_value_pages = IntSet.empty
		(** entrynodes to be added to index in next commit *)
		val mutable new_entry_nodes : entrynode list = []
    method get_version () = version
    (** read a node (page,offset) *)
    method read_node (pagenum:int) (offset:int) : node Lwt.t =
		  lwt nodepage = pagecache#lock_page pagenum in
			let rec find_node offset nodes =
				match nodes with 
					| [] -> raise (Failure "not found")
					| n :: ns -> if offset=(get_node_offset n) then n else find_node offset ns
			in
			try 
    		let node = find_node offset nodepage.nodes in
				noderefs <- NoderefSet.add (pagenum,offset) noderefs;
				pagecache#release_page pagenum false None;
				return node
			with
				| Failure msg -> begin
					  pagecache#release_page pagenum false None;
					  raise_lwt (Failure "not found")
					end
    (** read a value page [release??] *)
    method read_value_page (pagenum:int) : page Lwt.t = 
			lwt valuepage = pagecache#lock_page pagenum in
			read_value_pages <- IntSet.add pagenum read_value_pages;
			return valuepage
    (** release a value page from read_value_page *)
    method release_value_page (page:page) : unit = 
			if (IntSet.mem page.page read_value_pages) then begin
				pagecache#release_page page.page false None;
				read_value_pages <- IntSet.remove page.page read_value_pages
			end else 
				OS.Console.log (sprintf "release_value_page for unread page %d" page.page)
		(** allocate a new page, update rootnode, update lastnodepagenum if nodepage -> page is locked *)
		method private lock_new_page pagetype : page Lwt.t =
      lwt rootpage = pagecache#lock_page 0 in
      let Some rootnode = rootpage.rootnode in
			let lastpagenum = rootnode.rbody.lastpage+1 in
	    let newpagenum = lastpagenum in
      let lastnodepagenum = 
				if (pagetype=Nodepagetype) then newpagenum 
				else rootnode.rbody.lastnodepage in
			(* TODO: create without read... *)
			lwt newpage = pagecache#lock_page newpagenum in
			(* initialise as new page *)
			let pageheader = { pagetype = (int_of_pagetype pagetype); 
				pagesizelog2 = (sizelog2 pagecache#page_size) } in
			let pageheaderbits = bitstring_of_pageheader pageheader in
			bitstring_write pageheaderbits 0 newpage.data;
			(* leave locked *)
			(* update root node on root page *)
      lwt rootpage = pagecache#lock_page 0 in
			let newrootnode = { rootnode with rbody = { 
					rootnode.rbody with lastpage = lastpagenum; lastnodepage = lastnodepagenum } } in
			let newrootpage = { rootpage with rootnode = Some newrootnode } in
			let rootnodebits = bitstring_of_rootnodebody newrootnode.rbody in
			(* nb position in bytes *)
			bitstring_write rootnodebits rootnode.rnoffset rootpage.data; 
      pagecache#release_page 0 true (Some newrootpage);	
			OS.Console.log (sprintf "allocated new %s page %d" (string_of_pagetype pagetype) newpagenum);		
			return newpage
    (** get a value output page *)
    method new_value_page () : page Lwt.t = 
			lwt newpage = self#lock_new_page Valuepagetype in
			new_value_pages <- IntSet.add newpage.page new_value_pages;
			return newpage
    (** write a value output page (actually non-blocking at present) *)
    method write_value_page (page:page) : unit Lwt.t = 
			if (IntSet.mem page.page new_value_pages) then begin
				(* doesn't update parsed record - should only have written bytes into data after page header *)
				pagecache#release_page page.page true None;
				written_value_pages <- IntSet.add page.page written_value_pages;
				new_value_pages <- IntSet.remove page.page new_value_pages;
				return ()
			end else begin
				OS.Console.log (sprintf "release_value_page for unread page %d" page.page);
				return ()
			end
    (** write a node (nodesize,extrakey option,fn extrakeyoffset->node body) -> node *)
    method add_node (nodesize:int) (nodetype:int) (extrakey:bitstring option) (makenodebody:(int -> nodebody)) : node Lwt.t =
      (* which page can this be allocated in? *)
			(* just try the 'last node' page for now *)
			let extrakeylen = match extrakey with None -> 0 | Some bits -> ((bitstring_length bits)/8) in
      lwt rootpage = pagecache#lock_page 0 in
			(* not exhaustive but shouldn't fail *)
      let Some rootnode = rootpage.rootnode in
      let lastpagenum = rootnode.rbody.lastpage in
      let lastnodepagenum = rootnode.rbody.lastnodepage in
			(* TODO: check key is small enough for this DB blocksize *)
			(* try last page *)
			lwt nodepage = 
	 			lwt lastnodepage = pagecache#lock_page lastnodepagenum in
				if (is_room_for_node lastnodepage nodesize extrakeylen) then 
					return lastnodepage
			  else begin
					pagecache#release_page lastnodepagenum false None;
					(* last node page is no good; start a new page *)
					self#lock_new_page Nodepagetype
				end
			in
      (* write the extrakey *)
			let (keyoffset,extrakey) = match extrakey with 
				| Some extrakeybits -> begin
					  let extrakeyrec = add_extrakey nodepage extrakeybits in
						(extrakeyrec.koffset,
						 Some extrakeyrec)
				  end
				| None -> 
					(0,None) in
      (* write the node *)
			let nodebody = makenodebody keyoffset in
			let header = { size = nodesize; nodetype = nodetype } in
			let node = match nodebody with 
				| Rootnodebody b -> Rootnode { rnpage = nodepage.page; rnoffset = nodepage.pnodeoffset; rheader = header; rbody = b }
				| Entrynodebody b -> Entrynode { enpage = nodepage.page; enoffset = nodepage.pnodeoffset; eheader = header; ebody = b }
				| Btreenodebody b -> Btreenode { bnpage = nodepage.page; bnoffset = nodepage.pnodeoffset; bheader = header; bbody = b }
				| Valuenodebody b -> Valuenode { vnpage = nodepage.page; vnoffset = nodepage.pnodeoffset; vheader = header; vbody = b }
				| Unknownnodebody -> Unknownnode
			in
			let nodebits = bitstring_of_node node in
			let newnodepage = add_node nodepage node nodebits extrakey in 
			let noffset = get_node_offset node in
			OS.Console.log (sprintf "adding node type %d at %d/%d" nodetype nodepage.page noffset);
			(* add to noderefs *)
			noderefs <- NoderefSet.add (nodepage.page,noffset) noderefs;
			(* release/write pages *)
      pagecache#release_page nodepage.page true (Some newnodepage);
      pagecache#release_page 0 false None;
			(* return the new complete node record *)			
      return node
    (** add a new entry to the master index/version (node must be entry node), already 
      added. Blocks until (at least) this addition is in working version. *)
    method add_entry (node:entrynode) : unit Lwt.t = 
			(* just add to pending list *)
			new_entry_nodes <- node :: new_entry_nodes;
			return ()
	  (** new btree node *)
		method private new_btree_node level (lpage,loffset) (rpage,roffset) (bkeyprefix,bkeyextra) : node Lwt.t =
			let make_body bkeyoffset = Btreenodebody { 
				level; bkeyprefix; bkeyoffset; bkeyextra; lpage; loffset; rpage; roffset; 
			} in
			OS.Console.log (sprintf "Creating new btree node l=%d/%d r=%d/%d value=%s" lpage loffset rpage roffset (dump_of_keyvalue (bkeyprefix,bkeyextra)));
			self#add_node btreenodesize (btreenodetype+level-1) bkeyextra make_body 
    (** commit/release session *)
    method commit () : unit Lwt.t = 
			if (List.length new_entry_nodes)=0 then return () else begin
			(* sort new_entry_nodes by key *)
			new_entry_nodes <- List.sort compare_entrynode new_entry_nodes;			
			(* merge into existing btree *)
			(* we build up a stack of sub-trees, to which we, combining them as they match in level.*)
			(* we also keep track of where we are in the old btree *)
			(*let oldrootref = (version.vrootpage,version.vrootoffset) in*)
			(* TODO: concurrent updates? *)
      lwt rootpage = pagecache#lock_page 0 in
			(* not exhaustive but shouldn't fail *)
      let Some rootnode = rootpage.rootnode in
			let oldrootref = (rootnode.rbody.rootpage,rootnode.rbody.rootoffset) in
      pagecache#release_page 0 false None;	
			let get_node (page,offset) = self#read_node page offset in
			lwt bti = bti_new get_node oldrootref in
			(* Level 0 is an entity. *)
			let btb : btreebuildrec list = [] in
			(* recursive add: btreeiterator, newnodes, btreebuildrecs *)
			let rec add_enodes ((btieno,btirecs) as bti) newnodes (btbrecs: btreebuildrec list) : btreebuildrec list Lwt.t =
				(* actual add *)
				let add_enode bti btbrecs enode btirecs : btreebuildrec list Lwt.t =
					(* this is the next entrynode to add *)
					(* add it to the list.. *)
					let node = Entrynode enode in
					let keyvalue = get_keyvalue node in
					let btbr = { btbnode = node; btblevel = 0; btbmaxvalue = keyvalue; btbbtirecs = btirecs } in
					OS.Console.log (sprintf "Index node %d/%d level 0 value %s" enode.enpage enode.enoffset (dump_of_keyvalue keyvalue));
					let btbrecs = btbr :: btbrecs in
					(* now merge tail pair(s) at same level *)
					let rec merge_btbrecs btbrecs : btreebuildrec list Lwt.t = match btbrecs with
						| { btblevel = rlevel } as r :: ( { btblevel = llevel; btbbtirecs = btirecs } as l :: rest ) ->
							if (llevel=rlevel) then (
								(* merge... *)
								let lref = get_node_ref l.btbnode in
								let rref = get_node_ref r.btbnode in
								let level = llevel+1 in
								let value = l.btbmaxvalue in
								let maxvalue = r.btbmaxvalue in
								let new_btree_node () = self#new_btree_node level lref rref value in
								(* do we already have the required node in the bti? *)
								lwt (btn,btirecs) = match r.btbbtirecs with
									| { btinode = { 
												bbody = { lpage; loffset; rpage; roffset; level }
											} as oldbtn 
										} :: rest 
										when lpage = (fst lref) && loffset = (snd lref) &&  
													rpage = (fst rref) && roffset = (snd rref) && level = level 
										-> (
										  OS.Console.log (sprintf "Re-use btreenode %d/%d" oldbtn.bnpage oldbtn.bnoffset);
										  return (Btreenode oldbtn, rest) 
										)									
									| _ -> (* no - make a new one *) 
									  lwt nbtn = new_btree_node () in
										return (nbtn, []) 
								in let newbtn = { btblevel = level; btbnode = btn; btbbtirecs = btirecs; btbmaxvalue = maxvalue } in
								merge_btbrecs (newbtn :: rest)
							) else begin
								if (rlevel>llevel) then
								  OS.Console.log (sprintf "Bad btbrec list; top level %d > next level %d" rlevel llevel);
								return btbrecs
							end
						| _ -> return btbrecs
					in merge_btbrecs btbrecs 
				in match (btieno,newnodes) with
				  (* sort out whether next is from old btree index or new entries... *)
					| (None,[]) -> return btbrecs
					| (Some en,[]) -> begin
						  lwt btbrecs = add_enode bti btbrecs en btirecs in
							lwt bti = bti_advance get_node bti in
							add_enodes bti newnodes btbrecs
					  end
					| (None,en::ns) -> begin
						  lwt btbrecs = add_enode bti btbrecs en [] in
							add_enodes bti ns btbrecs
						end
					| (Some oen,nen::ns) -> begin
						  let cmp = compare_entrynode oen nen in
							if (cmp<=0) then begin
							  lwt btbrecs = add_enode bti btbrecs oen btirecs in
								lwt bti = bti_advance get_node bti in
								add_enodes bti newnodes btbrecs
							end else begin
							  lwt btbrecs = add_enode bti btbrecs nen [] in
								add_enodes bti ns btbrecs
							end					
						end
			in lwt btbrecs = add_enodes bti new_entry_nodes [] in
			(* now add partial btree nodes to collapse to a single entry *)
			let rec pmerge_btbrecs btbrecs : node option Lwt.t = match btbrecs with
				| [] -> return None
				| { btbnode = n } :: [] -> return (Some n)
				| r :: (l :: rest) -> begin
						let lref = get_node_ref l.btbnode in
						let rref = get_node_ref r.btbnode in
						let level = l.btblevel+1 in
						let value = l.btbmaxvalue in
						let maxvalue = r.btbmaxvalue in
						lwt nbtn = self#new_btree_node level lref rref value in
					  pmerge_btbrecs ({ btbnode = nbtn; btblevel = level; btbmaxvalue = maxvalue; btbbtirecs = [] }:: rest)
					end
			in lwt newroot = pmerge_btbrecs btbrecs in
			(* update root node *)
			let rootref = match newroot with
				| Some n -> (get_node_page n, get_node_offset n)
				| None -> (0,0)
			in 
			(* re-read/lock after all nodes added because that may change rootnode! *)
      lwt rootpage = pagecache#lock_page 0 in
			(* not exhaustive but shouldn't fail *)
      let Some rootnode = rootpage.rootnode in
			let newrootnode = { rootnode with rbody = { rootnode.rbody with rootpage = (fst rootref); rootoffset = (snd rootref) } } in
			let newrootpage = { rootpage with rootnode = Some newrootnode } in
			let rootnodebits = bitstring_of_rootnodebody newrootnode.rbody in
			(* nb position in bytes *)
			bitstring_write rootnodebits rootnode.rnoffset rootpage.data; 
      pagecache#release_page 0 true (Some newrootpage);	
			OS.Console.log (sprintf "Updated root to %d/%d" (fst rootref) (snd rootref));		
			return ()
			end
    (** abort *)
    method abort () : unit Lwt.t = raise_lwt (Failure "unimplemented")
	end 
and sessionmanagerimpl (_blkep : Blkifendpoint.blkendpoint) =
	object(mgr)
	  val blkep = _blkep;
		val pagecache = ((new pagecacheimpl _blkep) :> pagecache)
		val mutable sessions : sessionimpl list = []
		val mutable timestamp = Int32.of_float (OS.Clock.time ())
		val mutable sequence = 1L
    method get_version () = 
			lwt rootpage = pagecache#lock_page 0 in
			(* not exhaustive but shouldn't fail for root *)
			let Some rootnode = rootpage.rootnode in
			pagecache#release_page 0 false None;
			return { 
				vrootpage = rootnode.rbody.rootpage;
			  vrootoffset = rootnode.rbody.rootoffset; 
				vtimestamp = timestamp;  
				vsequence = sequence; 
   	  } 
		method get_session () : session Lwt.t = 
			lwt version = mgr#get_version () in
			let session = new sessionimpl ( mgr :> sessionmanagerimpl ) pagecache version in
			sessions <- session :: sessions;
			return (session :> session)
	end
(** btree iterator *)
and btreeiter (session:session) (root:noderef) =
	object (self)
	  val session = session
	  val root = root
	  val mutable t : btreeitert  = (None,[])
		val mutable is_new = true
		method peek () : entrynode option Lwt.t =
			(if (is_new) then 
				self#init() 
			else return ())
			>> return (fst t)
		method next () : entrynode option Lwt.t =
			(if (is_new) then 
				self#init()
			else return () 
			)>> 
			let node = fst t in
			(self#advance () >>
			return node)					
		method private go_left (page,offset) : unit Lwt.t =
			lwt nt = bti_go_left (fun (page,offset) -> session#read_node page offset) (page,offset) t in
			t <- nt; return()
		method private init() : unit Lwt.t =
			is_new <- false;
			self#go_left root
		method private advance () : unit Lwt.t =
			lwt nt = bti_advance (fun (page,offset) -> session#read_node page offset) t in
			t <- nt; return ()
	end

(** test session *)
let main() =
  lwt blkep = Blkifendpoint.get "test" in 
  let sessionmanager = new sessionmanagerimpl blkep in
	(* make a session and check the version *)
	lwt session = sessionmanager#get_session() in
	let version = session#get_version () in
	OS.Console.log (sprintf "version root=%d/%d timestamp=%ld sequence=%Ld" version.vrootpage version.vrootoffset version.vtimestamp version.vsequence);
	(* add an entry node *)
	let make_node_body timestamp sequence ekeyprefix ekeyextra valuepage valueoffset valuelength extrakeyoffset : nodebody =
		Entrynodebody { 
			ekeyprefix = ekeyprefix;
			ekeyoffset = extrakeyoffset;
			ekeyextra = ekeyextra;
			valuepage = valuepage;
			valueoffset = valueoffset;
			valuelength;
			timestamp = timestamp;
			sequence = sequence;
		} in
	let key = bitstring_of_string "abcdefghijkl" in
	let valuepage = 0 (*TODO*) in
	let valueoffset = 0 (*TODO*) in
	let valuelength = 0L (*TODO*) in
	let (keyprefix,extrakey) = split_key key in
	lwt entrynode = session#add_node entrynodesize entrynodetype extrakey (make_node_body (version.vtimestamp) (version.vsequence)
	  keyprefix extrakey valuepage valueoffset valuelength) in
  OS.Console.log (sprintf "added entry node at %d/%d" (get_node_page entrynode) (get_node_offset entrynode));
	(* get root of old btree (if any) *)
	let oldrootref = (version.vrootpage,version.vrootoffset) in
	let rec print_entry iter =
		lwt node = iter#next() in match node with 
			| None -> return ()
			| Some { enoffset; ebody = { 
              ekeyprefix; ekeyoffset; valuepage; ekeyextra;
              valueoffset; valuelength; timestamp; sequence } } ->
				begin
          let keydump = dump_of_keyvalue (ekeyprefix,ekeyextra) in
          OS.Console.log (sprintf "Entry node at %d with key %s@%d, value at %d:%d, %Ld bytes, timestamp %ld, sequence %Ld" 
              enoffset keydump ekeyoffset valuepage valueoffset valuelength timestamp sequence );
					print_entry iter
				end 
	in
  (* iterator for old btree *)
  let olditer = new btreeiter session oldrootref in
  OS.Console.log "Old entries...";
  print_entry olditer >>
	(* add entry *)
	(OS.Console.log "Add entrynode";
	(* partial... *)
	let Entrynode en = entrynode in 
	session#add_entry en >>
	(OS.Console.log "Commit";
	session#commit () >>
	(OS.Console.log "Done";
  return ()
	)))
	
(*---------------------------------------------------------------------------*)
(* test app - dump nodes in the root block *)

(** test - needs Blkif 'test' *)
let dumproot () =
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
	(*let get_extrakey page offset =
		if (offset=0) then empty_bitstring
		else 
			let rec find_extrakey extrakeys offset = match extrakeys with
				| [] -> empty_bitstring
				| { koffset = koffset ; extra = extra } :: tl -> 
					if (koffset=offset) then extra
					else find_extrakey tl offset in
			find_extrakey page.extrakeys offset
	in*)
	let Some rootnode = rootpage.rootnode in
  OS.Console.log (sprintf "Root node, root is %d:%d, last page is %d, last node page %d" 
          rootnode.rbody.rootpage rootnode.rbody.rootoffset rootnode.rbody.lastpage rootnode.rbody.lastnodepage);
	(* print nodes *)
	let print_node page n  = match n with
        | Rootnode { rnoffset; rbody = { 
              rootpage; rootoffset; lastpage } } ->
                OS.Console.log (sprintf "Root node at %d is %d:%d, last page is %d" 
          rnoffset rootpage rootoffset lastpage)
        | Entrynode { enoffset; ebody = { 
              ekeyprefix; ekeyoffset; ekeyextra; valuepage;
							valueoffset; valuelength; timestamp; sequence } } ->
								let keydump = dump_of_keyvalue (ekeyprefix, ekeyextra) in 
                OS.Console.log (sprintf "Entry node at %d with key %s@%d, value at %d:%d, %Ld bytes, timestamp %ld, sequence %Ld" 
                enoffset keydump ekeyoffset valuepage valueoffset valuelength timestamp sequence )
				| Btreenode { bnoffset; bbody = { bkeyprefix; bkeyoffset; bkeyextra; lpage; loffset; rpage; roffset } } ->
					let keydump = dump_of_keyvalue (bkeyprefix,bkeyextra) in
          OS.Console.log (sprintf "Bree node at %d with key %s@%d, left %d:%d, right %d:%d" 
            bnoffset keydump bkeyoffset lpage loffset rpage roffset )
				| Valuenode { vnoffset; vbody = { v0page; v0offset; v1page; v1offset; v2page; v2offset; v3page; v3offset } } ->
          OS.Console.log (sprintf "Value node at %d with children %d:%d, %d:%d, %d:%d, %d:%d" 
            vnoffset v0page v0offset v1page v1offset v2page v2offset v3page v3offset )
        | _ -> let noffset = get_node_offset n in
				  let header = get_node_header n in
	        OS.Console.log (sprintf "Node %d at %d" header.nodetype noffset) in
	List.iter (print_node rootpage) rootpage.nodes ;
	let rec print_entry iter =
		lwt node = iter#next() in match node with 
			| None -> return ()
			| Some { enoffset; ebody = { 
              ekeyprefix; ekeyoffset; valuepage; ekeyextra;
              valueoffset; valuelength; timestamp; sequence } } ->
				begin
          let keydump = dump_of_keyvalue (ekeyprefix,ekeyextra) in
          OS.Console.log (sprintf "Entry node at %d with key %s@%d, value at %d:%d, %Ld bytes, timestamp %ld, sequence %Ld" 
              enoffset keydump ekeyoffset valuepage valueoffset valuelength timestamp sequence );
					print_entry iter
				end 
	in
  (* iterator for old btree *)
  let sessionmanager = new sessionmanagerimpl blkep in
	(* make a session and check the version *)
	lwt session = sessionmanager#get_session() in
	let version = session#get_version () in
	let oldrootref = (version.vrootpage,version.vrootoffset) in
  let olditer = new btreeiter session oldrootref in
  OS.Console.log "Iterate entries...";
  print_entry olditer >>
  return ()

(*---------------------------------------------------------------------------*)
(* implementation of abstract simpleDB interface - skeleton *)

(** internal state *)
type t = {
    id : string;
    blkep : Blkifendpoint.blkendpoint;
		sessionmanager : sessionmanager
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
	let sessionmanager = ((new sessionmanagerimpl blkep) :> sessionmanager) in
	let t = { id = blkep#id; blkep = blkep; sessionmanager } in
  return (object
	  method id = blkep#id;
		method recv key : bytestreamin inresult Lwt.t = _recv t key ;
		method getbuf key : bytestreamout Lwt.t = _getbuf t key;
	end)
	
(*---------------------------------------------------------------------------*)
(* EOF *)