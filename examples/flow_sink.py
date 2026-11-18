# Prints every JSON body POSTed to it: the flow runner 1.0.0 has no API to read a workflow's output.
import http.server, json, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('content-length', 0)))
        try:
            text = json.dumps(json.loads(body), indent=2, ensure_ascii=False)
        except Exception:
            text = body.decode(errors='replace')
        print(f"=== {self.path}\n{text}", flush=True)
        self.send_response(200); self.send_header("content-type","application/json"); self.end_headers(); self.wfile.write(b"{}")
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(('127.0.0.1', 8099), H).serve_forever()
