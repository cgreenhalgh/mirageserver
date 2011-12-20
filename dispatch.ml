(* Http server request handler, based on mirage-tutorial example. *)
open Lwt
open Regexp

(* Dynamic Dispatch *)
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
    dyn ~headers:content_type_xhtml req (return (Cow.Html.to_string xhtml))

  (** return 404 error *)
  let not_found path = 
    Log.info "HTTP" "Not found: %s\n" path;
    Http.Server.respond_not_found ~url:path ()

  (** xhtml of index page - test example *)
  let index = 
    <:html< <html>
              <head><title>Index</title></head>
              <body>Hello World!</body>
    </html> >>

  (** the request handling entry point *)
  let dispatch req =
    function
    | "/index.html" 
    | "/"           -> dyn_xhtml req index
    | x             -> not_found (Http.Request.path req)

end

(* handle exceptions with a 500 *)
let exn_handler exn =
  let body = Printexc.to_string exn in
  Log.error "HTTP" "ERROR: %s" body;
  return ()

(* main callback function *)
let t conn_id req =
  let path = Http.Request.path req in
  (* just pass to Dynamic.dispatch - above *)
  Dynamic.dispatch req path
