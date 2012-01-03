(* First tinkering with I/O endpoint abstraction, based on discussions *)
(* with Mort and Anil. *)
(* Chris Greenhalgh, The University of Nottingham, 2012-03-01 *)

(*open Lwt*)

(* manage endpoints, e.g. create them. Also plug in providers? *)
module Manager = 
	struct
	end
	
module type Genin =
	sig
		type t
		type datat
		type inmetat
		type inbufmetat
		type inbuf 
		val recv: t -> inmetat -> (datat,inbufmetat,inbuf) Lwt.t 
		val release: inbuf -> unit Lwt.t
  end		

module type Bytestreamin = Genin with
		type datat = Bitstream.bitstring and
		type inmetat = int (* pref size *) and
		type inbufmetat = unit and
		type inbuf = unit (* no-op? *)
	

		