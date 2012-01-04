(* First tinkering with I/O endpoint abstraction, based on discussions *)
(* with Mort and Anil. *)
(* Chris Greenhalgh, The University of Nottingham, 2012-03-01 *)

(*open Lwt*)

(* manage endpoints, e.g. create them. Also plug in providers?, as class *)
(*type manager = <
>*)

(** in (recv) return type *)
type 'a inresult = 
      Data of 'a
    | End (* Could be Complete or Error - unknown reason *)
    | Complete (* definitely complete data *)
    | Error (* definitely an error, possibly incomplete data *)

(** in/out exception - timeout *)
exception Timeout

(** in/out exception - metadata invalid *)
exception InvalidRequest of string

(** general input buffer, as class *)
type inbuf = <
    (** release buffer and underlying resources *)
    release: unit -> unit
>
(** fully generalised buffered input - not sure how to do this type-wise at the mo *) 
(* type 'data 'recvmeta 'inbufmeta 'inbuf genin = <
    (** blocking read (stream, pref-size) -> (data,(),())
     * raise Timeout *)
    recv: 'recvmeta -> ('data * 'inbufmeta * 'inbuf) inresult Lwt.t; 
    (** release buffer - generic signature; non-blocking?! *)
    release: 'inbuf -> unit;
    (** release/discard stream, e.g. close underlying stream *)
    discardin: unit -> unit    
> *)
(** bytestream input, using Bitstring.t as buffer type, as class *)
type bytestreamin = <
    (** blocking read (stream, pref-size) -> (data,(),())
     * raise Timeout, InvalidRequest *)
    recv: int -> (Bitstring.t * unit * inbuf) inresult Lwt.t; 
    (** release/discard stream, e.g. close underlying stream *)
    discardin: unit -> unit    
>

(** out exception - stream full / exhausted *)
exception OutFull

(** out exception - stream closed/completed (near end) *)
exception OutCompleted

(** out exception - stream discarded (force close) (near end) *)
exception OutDiscarded

(** out exception - other error, e.g. underlying stream, far end close *)
exception OutError

(** generic out buffer, as class *)
type 'sendmeta outbuf = <
    (** send (metadata). 
     * raise Timeout, OutFull (finite stream), OutCompleted, OutDiscarded, OutError,
		 * InvalidRequest. *)
    send: 'sendmeta -> unit Lwt.t;
    (** release buffer and underlying resources - only if not sending! *)
    release: unit -> unit
>
(** fully generalised buffered output, as class; not sure how to do this type-wise at the mo *)
(* type genout = < 
    (** blocking get buffer for send, (stream, pref-size) -> (data, (), outbuf)
     *  may block. 
     * raise Timeout, OutFull (finite stream), OutCompleted, OutDiscarded, OutError. *)
    val getbuf : t -> getbufmeta -> (data * outbufmeta * outbuf) Lwt.t
    (** release/discard stream, e.g. close underlying stream *)
    val discardout: t -> unit       
  >
*)
(** bytestream out buffer, as class *)
type bytestreamoutbuf = <
    (** send (complete,actual-size). 
     * raise Timeout, OutFull (finite stream), OutCompleted, OutDiscarded, OutError,
     * InvalidRequest. *)
    send: (bool * int) -> unit Lwt.t;
    (** release buffer and underlying resources - only if not sending! *)
    release: unit -> unit
>
    
(** bytestream output, using Bitstring.t as buffer type, as class *)
type bytestreamout = <
    (** blocking get buffer for send, (stream, pref-size) -> (data, (), outbuf)
     *  may block. 
     * raise Timeout, OutFull (finite stream), OutCompleted, OutDiscarded, OutError,
     * InvalidRequest. *)
    getbuf : int -> (Bitstring.t * unit * bytestreamoutbuf) Lwt.t;
    (** release/discard stream, e.g. close underlying stream *)
    discardout : unit -> unit       
>	
