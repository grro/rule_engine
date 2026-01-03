import json
import threading
import logging
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any
from device import Store



class SimpleRequestHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # suppress access logging
        pass

    def do_GET(self):
        store: Store = self.server.store
        parsed_url = urlparse(self.path)
        path = parsed_url.path.lstrip("/")

        if path in store.property_names:
            query_params = parse_qs(parsed_url.query)

            if 'set' in query_params:
                new_value = query_params['set'][0]
                old_value = store.get_property(path)
                if type(old_value) == bool:
                    new_value = True if new_value in {"true", 'True', 'TRUE'} else False
                elif type(old_value) == int:
                    new_value = int(new_value)

                store.set_property(path, new_value) # Assuming set_property exists in Store
                self._send_json(200, {"status": "success", "name": path, "value": new_value})
            else:
                value = store.get_property(path)
                self._send_json(200, {'name': path, 'value': value})

        else:
            html = "<h1>Available names</h1><ul>"
            for name in store.property_names:
                html += f"<li><a href='/{name}'>{name}</a></li>"
            html += "</ul>"
            self._send_html(200, html)


    def _send_html(self, status, message):
        self.send_response(status)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(message.encode("utf-8"))

    def _send_json(self, status, data: Dict[str, Any]):
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def _send_text(self, status, data: str):
        self.send_response(status)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(data.encode("utf-8"))

class StoreWebServer:
    def __init__(self, store: Store,  host='0.0.0.0', port=8000):
        self.host = host
        self.port = port
        self.address = (self.host, self.port)
        self.server = HTTPServer(self.address, SimpleRequestHandler)
        self.server.store = store
        self.server_thread = None

    def start(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        logging.info(f"web server started http://{self.host}:{self.port}")

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        logging.info("web server stopped")

