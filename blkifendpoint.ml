(* Blkif endpoint abstraction. Mirrors generic abstraction structure*)
(* in enpoint.ml. Implementation over OS.Device.blkif *)
(* Chris Greenhalgh, University of Nottingham, 2012-01-04 *)
open Endpoint

open Lwt
open Printf
open Bitstring

(** general input abstraction over a block device, as class *)
type blkin = <
    (** blocking read (blocknumber) -> (data,(),inbuf)
     * raise Timeout,
     * InvalidRequest (e.g. out of range block) *)
    recv: int -> (Bitstring.t * unit * inbuf) inresult Lwt.t; 
    (** release/discard device *)
    discardin: unit -> unit;
    (** size of block *)
    block_size : int          
>

(** block out buffer, as class *)
type blkoutbuf = <
    (** send (). 
     * raise Timeout, OutFull (finite stream), OutCompleted, OutDiscarded, OutError,
     * InvalidRequest. *)
    send: unit -> unit Lwt.t;
    (** release buffer and underlying resources - only if not sending! *)
    release: unit -> unit
>

(** general output abstraction over a block device *)
type blkout = <
    (** blocking get buffer for send, (stream, pref-size) -> (data, (), outbuf)
     *  may block. 
     * raise Timeout, OutFull (finite stream), OutCompleted, OutDiscarded, OutError,
     * InvalidRequest (e.g. out of range block). *)
    getbuf : int -> (Bitstring.t * unit * blkoutbuf) Lwt.t;
    (** release/discard stream, e.g. close underlying stream *)
    discardout : unit -> unit;
		(** size of block *)
		block_size : int       
>

type blkendpoint = <
  (** blkif id *)
  id : string;
  (** in *)
  blkin : blkin;
	(** out *)
	blkout : blkout;
  (** size of block *)
  block_size : int      
>

(** internal state of endpoint *)
type t = {
	id : string;
	blkif : OS.Devices.blkif;
}

(** inbuf with no-op release method *)
let noop_inbuf = (object
    method release () = ()
  end)

(** implementation of blkin recv *)
let _recv t block = 
	lwt data = t.blkif#read_page 
	   (Int64.mul (Int64.of_int block) (Int64.of_int t.blkif#sector_size)) in
	return (Data (data, (), noop_inbuf))

(** implementation of blkin/out block_size *)
let _block_size t = t.blkif#sector_size

(** implementation of blkbufout send *)
let _send t block data =
	t.blkif#write_page (Int64.mul (Int64.of_int block) (Int64.of_int t.blkif#sector_size)) data

(** implementation of blkout getbuf *)		
let _getbuf t block =
	let data = Bitstring.create_bitstring (8*t.blkif#sector_size) in
	let buf : blkoutbuf = (object
	  (* no-op *)
	  method release () = ();
		(* send! *)
		method send () = _send t block data
	end) in
	return (data, (), buf)

(** get named blk endpoint *)
let get id : blkendpoint Lwt.t =
  lwt blkif = match_lwt (OS.Devices.find_blkif id) with
        | Some blkif -> return blkif
        | None -> raise_lwt Not_found
  in 
  OS.Console.log (sprintf "OK... name %s, sector size %d" blkif#id blkif#sector_size);
	let t = { id = blkif#id; blkif = blkif } in
	let blkin = (object
    method recv block = _recv t block;
    method discardin () = ();
    method block_size = _block_size t
	end) in
	let blkout = (object 
    method getbuf block = _getbuf t block;
	  method discardout()  = ();
    method block_size = _block_size t
	end) in
  return (object 
	  method id = blkif#id;
		method blkin : blkin = blkin ;
		method blkout : blkout = blkout ;
		method block_size = blkif#sector_size
	end)	
	

(** test - find/print block device 'test'.
 * For unix-socket you will need to create a file for the VBD, e.g. 
 *  dd if=/dev/zero of=test.vbd bs=1024 count=1024
 * and then specify it on the command-line when running the app, e.g. 
 * ./_build/unix-socket/blkifendpoint.bin -vbd test:test.vbd
 *)

let main () =
	OS.Console.log "Looking for block device 'test'...";
	(* get/open the "test" block device as a block endpoint *)
	lwt blkep = get "test" in 
  OS.Console.log (sprintf "OK... name %s, block size %d" blkep#id blkep#block_size);
	(* read block 0 *)
	lwt (indata,inbuf) = match_lwt (blkep#blkin#recv 0) with
		| Data (data,(),buf) -> return (data,buf)
		| _ -> OS.Console.log (sprintf "Error reading block"); 
		  raise_lwt ( Failure "reading block" ) in
	(* getbuf to write block 0 *)
	lwt (outdata,(),outbuf) = blkep#blkout#getbuf 0 in
	(* copy read data to writing data *)
	bitstring_write indata 0 outdata;
	(* extract 32-bit count from start of read data *)
	let count = bitmatch indata with 
		| { count : 32 } -> (Int32.add count 1l) in
	(* now done with inbuf (null op, though) *)
	inbuf#release ();
	(* add one and re-encode in bitstring *)
	let nbs = BITSTRING { count : 32 } in
	(* over-write start of output block with encoded data *)
	bitstring_write nbs 0 outdata;
	(* write to disk *)
	outbuf#send () >>
	(* happy :) *)
  ( OS.Console.log (sprintf "written count=%ld" count);
	  return () )
