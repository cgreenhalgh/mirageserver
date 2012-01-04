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
 *   size [30] : 16 : unsigned;
 *   nodetype [64] : 16 : unsigned; 
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

(** bits of key in prefix *)
let keprefixlength = 64
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
	koffset : int;
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
	    ekeyprefix : 64 : bitstring; (* NB not sure if the actual length is important *)
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
	(* data starts at current offset! *)
	let rec read_nodes data offset = bitmatch data with
		{ size : 16 : unsigned, littleendian;
			nodetype : 16 : unsigned, littleendian
	  } -> 
			if (size*8 > bitstring_length data) then
	   	  raise (Failure (sprintf "node size too big (%d/%d)" (bitstring_length data) (size*8)))
      else if (size==0) then
			  (* end *) ([],offset)
			else match (read_nodes (dropbits (size*8) data) (offset+size)) with
					(ns,offset) -> (parse_node data offset size nodetype :: ns, offset)
	in
	(* parse nodes *)
	let (nodes,pnodeoffset) = read_nodes data 0 in
	(* TODO: parse extra keys *)
	{ page = page; data = data; nodes = nodes; pnodeoffset = pnodeoffset;
	  extrakeys = []; pextrakeyoffset = (bitstring_length data)/8-2 }
		
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
    n.ekeyprefix : 64 : bitstring; (* NB not sure if the actual length is important *)
    n.ekeyoffset : 16 : unsigned;
    0 : 2;
    n.valuepage : 30 : unsigned;
    n.valueoffset : 16 : unsigned;
    n.timestamp : 32;
    n.sequence : 64
	}


(** ininitialise the root page with a root node, only *)
let create_rootpage size = 
	let rootnode = create_rootnode 0 0 0 in
	concat [ rootnode; zeroes_bitstring ((size*8)-bitstring_length rootnode) ]

(** danger - clobber/initialise root page on specified device *)
let init_device name = 
	OS.Console.log (sprintf "WARNING: initialising block device %s as empty simpleDB" name);
	OS.Time.sleep 3.0 >>
  lwt blkep = (Blkifendpoint.get "test") in 
  OS.Console.log (sprintf "Got Blkif name %s, sector size %d" blkep#id blkep#block_size);
  let rootpage = create_rootpage blkep#block_size in
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


(** test - needs Blkif 'test' *)
let main () =
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
	(* print nodes *)
	let print n = match n with
        | { noffset = noffset; body = Rootnode { 
              rootpage=rootpage; rootoffset=rootoffset; lastpage=lastpage } } ->
                OS.Console.log (sprintf "Root node at %d is %d:%d, last page is %d" 
          noffset rootpage rootoffset lastpage)
        | { noffset = noffset; header = { nodetype = nodetype } } ->
               OS.Console.log (sprintf "Node %d at %d" nodetype noffset) in
	List.iter print rootpage.nodes;
	(* TODO: create and add an entry node, initially no extra key *)
	(* TODO: enough space in this page? if not allocate a new free page (update last) *)
	(* TODO: create entrynode record; convert to bitstring; copy into current free*)
	(* space in block; schedule write back; update cache *)
	return ()
