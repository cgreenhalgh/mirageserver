(* Http server request handler, based on mirage-tutorial example. *)
open Lwt
open Regexp
(* the next is required for the json syntax extension, which needs Json module *)
open Cow

(** string list with the type-safe Json marshalling stuff generated *)
type session_list = string list with json

(** Dispatch of all (dynamic) http requests done by this module *)
module Dynamic = struct

  (** return OK response with given body and optional header(s) *)
  let dyn ?(headers=[]) req body =
    Log.info "HTTP" "Dispatch: dynamic URL %s\n%!" (Http.Request.path req);
    lwt body = body in
    let status = `OK in
    Http.Server.respond ~body ~headers ~status ()

  (** http header value for standard xhtml content mime type *)
  let content_type_xhtml = ["content-type","text/html"]

  (** return OK response where body is Cow.Html (xhtml) *)
  let dyn_xhtml req xhtml =
    dyn ~headers:content_type_xhtml req  (return (Cow.Html.to_string xhtml))

  (** http header value for standard xhtml content mime type *)
  let content_type_json = ["content-type","application/json"]

  (** return OK response where body is Json.t *)
  let dyn_json req json =
    dyn ~headers:content_type_json req (return (Json.to_string json))

  (** return 404 error *)
  let not_found path = 
    Log.info "HTTP" "Not found: %s\n" path;
    Http.Server.respond_not_found ~url:path ()

  (** return a specific status - presumably an error *)
  let error (status:[< Http.Types.status]) path = 
		let code = Http.Common.code_of_status status in
    Log.info "HTTP" "Error: %n %s\n" code path;
    Http.Server.respond_error ~status:(`Code code) ()

	(** xhtml of index page - test example *)
  let index = 
    <:html< <html>
              <head><title>Index</title></head>
              <body>Hello World!</body>
    </html> >>

  (** get sessions *)
	let get_sessions store req = 
		lwt names = Store.list store in
		dyn_json req ( json_of_session_list names )

  (** add a session *)
	let add_session store req =
		let bodylist = Http.Request.body req in
		match bodylist with
			(* Agh... POST body handling is currently not implemented (commented out*)
			(* and marked 'TODO') in mirage's version of http *)
			| [ `String s ] -> Log.info "HTTP" "Body %s (%n)" s (String.length s);
				error `Not_implemented (Http.Request.path req)
			| [ `Inchan (size, stream) ] -> Log.info "HTTP" "Body stream (%Ld)" size;
				error `Not_implemented (Http.Request.path req)
			| x -> error `Bad_request (Http.Request.path req)

	(** dispatch session/ (exactly) *)
  let dispatch_sessions store req  =
		match Http.Request.meth req with
			| `GET -> get_sessions store req 
			| `POST -> add_session store req
			| x -> error `Method_not_allowed (Http.Request.path req)
					
	(** dispath all session/... URLs *)
  let dispatch_session store req pathels = 
		match pathels with 
		| [""] -> dispatch_sessions store req
		| x  -> not_found (Http.Request.path req)
		
  (** the request handling entry point *)
  let dispatch store req =
    let path = Http.Request.path req in
    let pathels = match Re.split_delim (Re.from_string "/") path with
			| ""::tl -> tl 
			| x -> x in
    match pathels with
    | ["index.html"] 
    | [""]          -> dyn_xhtml req index
		| "session"::tl -> dispatch_session store req tl
    | x             -> not_found (Http.Request.path req)

end

(** handle exceptions with a 500 *)
let exn_handler exn =
  let body = Printexc.to_string exn in
  Log.error "HTTP" "ERROR: %s" body;
  return ()

(** main callback function, called by server *)
let t store conn_id req =
  (* just pass to Dynamic.dispatch - above *)
  Dynamic.dispatch store req 
