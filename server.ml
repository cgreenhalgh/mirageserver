(* Standard HTTP server core application.
   Based on Mirage tutorial, 2011-12-20.
   Delegates request handling to Dispatch.t *) 

open Lwt

(** default server port *)
let port = 8080

(** default static IP - for direct *)
let ip =
  let open Net.Nettypes in
  ( ipv4_addr_of_tuple (10l,0l,0l,2l),
    ipv4_addr_of_tuple (255l,255l,255l,0l),
   [ipv4_addr_of_tuple (10l,0l,0l,1l)]
  )

(** main application function *)
let main () =
  Log.info "Server" "listening to HTTP on port %d" port;
  lwt store = Store.create in
  let callback = Dispatch.t store in
  (* http server configuration *)
  let spec = {
    (* bind address *)
    Http.Server.address = "0.0.0.0";
    auth = `None;
    (* request handler *)
    callback;
    (* handle close - no-op *)
    conn_closed = (fun _ -> ());
    (* bind port *)
    port;
    (* exception handler *)
    exn_handler = Dispatch.exn_handler;
    (* timeout for request handler *)
    timeout = Some 300.;
  } in
  Log.info "Server" "starting HTTP server";
  Net.Manager.create (fun mgr interface id ->
    let src = None, port in
    (* configure network interface - doesn't do much on socket runtime *)
    Net.Manager.configure interface (`IPv4 ip) >>
    (* start the actual server listening *)
    Http.Server.listen mgr (`TCPv4 (src, spec))
  )
