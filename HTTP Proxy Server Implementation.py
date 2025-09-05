#!/usr/bin/env python3


import socket
import sys
import threading
import time
import re
from datetime import datetime
from urllib.parse import urlparse
from collections import OrderedDict

class HTTPMessage:
    """Class to represent and parse HTTP messages"""
    
    def __init__(self):
        self.start_line = ""
        self.headers = {}
        self.body = b""
        self.raw_headers = []
    
    def parse_request(self, data):
        """Parse HTTP request message"""
        lines = data.split(b'\r\n')
        
        # Parse start line
        self.start_line = lines[0].decode('utf-8', errors='ignore')
        parts = self.start_line.split(' ')
        if len(parts) != 3:
            raise ValueError("Invalid request line")
        
        self.method = parts[0]
        self.target = parts[1]
        self.version = parts[2]
        
        # Parse headers
        i = 1
        while i < len(lines) and lines[i] != b'':
            header_line = lines[i].decode('utf-8', errors='ignore')
            self.raw_headers.append(header_line)
            
            if ':' in header_line:
                name, value = header_line.split(':', 1)
                self.headers[name.strip().lower()] = value.strip()
            i += 1
        
        # Parse body (everything after empty line)
        if i + 1 < len(lines):
            self.body = b'\r\n'.join(lines[i + 1:])
        
        return self
    
    def parse_response(self, data):
        """Parse HTTP response message"""
        lines = data.split(b'\r\n')
        
        # Parse status line
        self.start_line = lines[0].decode('utf-8', errors='ignore')
        parts = self.start_line.split(' ', 2)
        if len(parts) < 2:
            raise ValueError("Invalid response line")
        
        self.version = parts[0]
        self.status_code = int(parts[1])
        self.reason_phrase = parts[2] if len(parts) > 2 else ""
        
        # Parse headers
        i = 1
        while i < len(lines) and lines[i] != b'':
            header_line = lines[i].decode('utf-8', errors='ignore')
            self.raw_headers.append(header_line)
            
            if ':' in header_line:
                name, value = header_line.split(':', 1)
                self.headers[name.strip().lower()] = value.strip()
            i += 1
        
        # Parse body
        if i + 1 < len(lines):
            self.body = b'\r\n'.join(lines[i + 1:])
        
        return self

class HTTPCache:
    """LRU Cache for HTTP responses"""
    
    def __init__(self, max_object_size, max_cache_size):
        self.max_object_size = max_object_size
        self.max_cache_size = max_cache_size
        self.cache = OrderedDict()
        self.current_size = 0
        self.lock = threading.Lock()
    
    def normalize_url(self, url):
        """Normalize URL for cache key"""
        parsed = urlparse(url)
        
        # Default port handling
        port = parsed.port
        if port is None:
            port = 80 if parsed.scheme.lower() == 'http' else 443
        
        # Default path handling
        path = parsed.path if parsed.path else '/'
        
        # Case insensitive scheme and host, case sensitive path and query
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname.lower() if parsed.hostname else ''
        query = parsed.query
        
        normalized = f"{scheme}://{hostname}:{port}{path}"
        if query:
            normalized += f"?{query}"
        
        return normalized
    
    def get(self, url):
        """Get cached response"""
        with self.lock:
            key = self.normalize_url(url)
            if key in self.cache:
                # Move to end (most recently used)
                response = self.cache.pop(key)
                self.cache[key] = response
                return response
            return None
    
    def put(self, url, response_data):
        """Cache response with LRU eviction"""
        with self.lock:
            # Check if response is cacheable
            if len(response_data) > self.max_object_size:
                return
            
            key = self.normalize_url(url)
            
            # Remove existing entry if present
            if key in self.cache:
                old_size = len(self.cache[key])
                self.current_size -= old_size
                del self.cache[key]
            
            # Evict least recently used items if necessary
            while self.current_size + len(response_data) > self.max_cache_size and self.cache:
                lru_key, lru_data = self.cache.popitem(last=False)
                self.current_size -= len(lru_data)
            
            # Add new item
            if self.current_size + len(response_data) <= self.max_cache_size:
                self.cache[key] = response_data
                self.current_size += len(response_data)

class HTTPProxy:
    """Main HTTP Proxy Server"""
    
    def __init__(self, port, timeout, max_object_size, max_cache_size):
        self.port = port
        self.timeout = timeout
        self.cache = HTTPCache(max_object_size, max_cache_size)
        self.server_socket = None
    
    def start(self):
        """Start the proxy server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(5)
        
        print(f"HTTP Proxy listening on port {self.port}")
        
        try:
            while True:
                client_socket, client_address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()
        except KeyboardInterrupt:
            print("\nShutting down proxy...")
        finally:
            if self.server_socket:
                self.server_socket.close()
    
    def handle_client(self, client_socket, client_address):
        """Handle client connection"""
        client_socket.settimeout(self.timeout)
        
        try:
            while True:
                # Receive request from client
                request_data = self.receive_http_message(client_socket)
                if not request_data:
                    break
                
                try:
                    request = HTTPMessage().parse_request(request_data)
                    self.process_request(client_socket, client_address, request, request_data)
                    
                    # Check if connection should be closed
                    connection_header = request.headers.get('connection', '').lower()
                    proxy_connection_header = request.headers.get('proxy-connection', '').lower()
                    
                    # Determine if client wants to close connection
                    if connection_header == 'close' or proxy_connection_header == 'close':
                        break
                    
                except Exception as e:
                    print(f"Error processing request: {e}")
                    self.send_error_response(client_socket, 400, "Bad Request", "invalid request")
                    break
        
        except socket.timeout:
            pass  # Client timeout, close connection
        except Exception as e:
            print(f"Client handler error: {e}")
        finally:
            client_socket.close()
    
    def receive_http_message(self, sock):
        """Receive complete HTTP message"""
        data = b""
        
        try:
            # Read until we get headers
            while b'\r\n\r\n' not in data:
                chunk = sock.recv(1024)
                if not chunk:
                    return None
                data += chunk
            
            # Split headers and potential body
            headers_end = data.find(b'\r\n\r\n')
            headers_data = data[:headers_end + 4]
            body_data = data[headers_end + 4:]
            
            # Parse headers to determine if there's a body
            headers_str = headers_data.decode('utf-8', errors='ignore')
            content_length = 0
            has_transfer_encoding = False
            
            for line in headers_str.split('\r\n')[1:]:  # Skip start line
                if ':' in line:
                    name, value = line.split(':', 1)
                    name = name.strip().lower()
                    value = value.strip()
                    
                    if name == 'content-length':
                        content_length = int(value)
                    elif name == 'transfer-encoding':
                        has_transfer_encoding = True
            
            # Read body if needed
            if content_length > 0:
                while len(body_data) < content_length:
                    chunk = sock.recv(min(1024, content_length - len(body_data)))
                    if not chunk:
                        break
                    body_data += chunk
            
            return headers_data + body_data
            
        except Exception as e:
            print(f"Error receiving message: {e}")
            return None
    
    def process_request(self, client_socket, client_address, request, request_data):
        """Process HTTP request"""
        method = request.method
        target = request.target
        
        # Log the request
        timestamp = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
        
        if method == 'CONNECT':
            self.handle_connect(client_socket, client_address, request, timestamp)
        elif method in ['GET', 'HEAD', 'POST']:
            self.handle_http_request(client_socket, client_address, request, request_data, timestamp)
        else:
            self.send_error_response(client_socket, 405, "Method Not Allowed", "method not allowed")
    
    def handle_connect(self, client_socket, client_address, request, timestamp):
        """Handle CONNECT method for HTTPS tunneling"""
        target = request.target
        
        # Validate CONNECT request
        if ':' not in target:
            self.send_error_response(client_socket, 400, "Bad Request", "invalid port")
            self.log_request(client_address, "-", timestamp, request.start_line, 400, 0)
            return
        
        host, port_str = target.rsplit(':', 1)
        try:
            port = int(port_str)
        except ValueError:
            self.send_error_response(client_socket, 400, "Bad Request", "invalid port")
            self.log_request(client_address, "-", timestamp, request.start_line, 400, 0)
            return
        
        if port != 443:
            self.send_error_response(client_socket, 400, "Bad Request", "invalid port")
            self.log_request(client_address, "-", timestamp, request.start_line, 400, 0)
            return
        
        # Check for self-loop
        if self.is_self_loop(host, port):
            self.send_error_response(client_socket, 421, "Misdirected Request", "proxy address")
            self.log_request(client_address, "-", timestamp, request.start_line, 421, 0)
            return
        
        try:
            # Connect to target server
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.settimeout(self.timeout)
            server_socket.connect((host, port))
            
            # Send 200 Connection Established
            response = "HTTP/1.1 200 Connection Established\r\n\r\n"
            client_socket.send(response.encode())
            
            self.log_request(client_address, "-", timestamp, request.start_line, 200, 0)
            
            # Start tunneling
            self.tunnel_data(client_socket, server_socket)
            
        except socket.gaierror:
            self.send_error_response(client_socket, 502, "Bad Gateway", "could not resolve")
            self.log_request(client_address, "-", timestamp, request.start_line, 502, 0)
        except socket.timeout:
            self.send_error_response(client_socket, 504, "Gateway Timeout", "timed out")
            self.log_request(client_address, "-", timestamp, request.start_line, 504, 0)
        except ConnectionRefusedError:
            self.send_error_response(client_socket, 502, "Bad Gateway", "connection refused")
            self.log_request(client_address, "-", timestamp, request.start_line, 502, 0)
        except Exception as e:
            print(f"CONNECT error: {e}")
            self.send_error_response(client_socket, 502, "Bad Gateway", "connection failed")
            self.log_request(client_address, "-", timestamp, request.start_line, 502, 0)
    
    def tunnel_data(self, client_socket, server_socket):
        """Bidirectional data tunneling for CONNECT"""
        def forward_data(source, destination):
            try:
                while True:
                    data = source.recv(4096)
                    if not data:
                        break
                    destination.send(data)
            except:
                pass
            finally:
                try:
                    source.close()
                    destination.close()
                except:
                    pass
        
        # Start forwarding threads
        client_to_server = threading.Thread(target=forward_data, args=(client_socket, server_socket))
        server_to_client = threading.Thread(target=forward_data, args=(server_socket, client_socket))
        
        client_to_server.daemon = True
        server_to_client.daemon = True
        
        client_to_server.start()
        server_to_client.start()
        
        # Wait for both threads to finish
        client_to_server.join()
        server_to_client.join()
    
    def handle_http_request(self, client_socket, client_address, request, request_data, timestamp):
        """Handle GET, HEAD, POST requests"""
        method = request.method
        target = request.target
        
        # Parse target URL
        if not target.startswith('http://'):
            self.send_error_response(client_socket, 400, "Bad Request", "no host")
            self.log_request(client_address, "-", timestamp, request.start_line, 400, 0)
            return
        
        parsed_url = urlparse(target)
        host = parsed_url.hostname
        port = parsed_url.port if parsed_url.port else 80
        path = parsed_url.path if parsed_url.path else '/'
        if parsed_url.query:
            path += '?' + parsed_url.query
        
        if not host:
            self.send_error_response(client_socket, 400, "Bad Request", "no host")
            self.log_request(client_address, "-", timestamp, request.start_line, 400, 0)
            return
        
        # Check for self-loop
        if self.is_self_loop(host, port):
            self.send_error_response(client_socket, 421, "Misdirected Request", "proxy address")
            self.log_request(client_address, "-", timestamp, request.start_line, 421, 0)
            return
        
        cache_status = "-"
        
        # Check cache for GET requests
        if method == 'GET':
            cached_response = self.cache.get(target)
            if cached_response:
                cache_status = "H"
                client_socket.send(cached_response)
                
                # Parse response to get status and body length
                try:
                    response = HTTPMessage().parse_response(cached_response)
                    body_length = len(response.body)
                    self.log_request(client_address, cache_status, timestamp, request.start_line, response.status_code, body_length)
                except:
                    self.log_request(client_address, cache_status, timestamp, request.start_line, 200, 0)
                return
            else:
                cache_status = "M"
        
        # Forward request to origin server
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.settimeout(self.timeout)
            server_socket.connect((host, port))
            
            # Transform request
            transformed_request = self.transform_request(request, path)
            server_socket.send(transformed_request)
            
            # Receive response
            response_data = self.receive_http_message(server_socket)
            server_socket.close()
            
            if response_data:
                # Parse response
                response = HTTPMessage().parse_response(response_data)
                
                # Transform response
                transformed_response = self.transform_response(response, request)
                
                # Cache GET responses with status 200
                if method == 'GET' and response.status_code == 200:
                    self.cache.put(target, transformed_response)
                
                # Send to client
                client_socket.send(transformed_response)
                
                body_length = len(response.body)
                self.log_request(client_address, cache_status, timestamp, request.start_line, response.status_code, body_length)
            else:
                self.send_error_response(client_socket, 502, "Bad Gateway", "closed unexpectedly")
                self.log_request(client_address, cache_status, timestamp, request.start_line, 502, 0)
        
        except socket.gaierror:
            self.send_error_response(client_socket, 502, "Bad Gateway", "could not resolve")
            self.log_request(client_address, cache_status, timestamp, request.start_line, 502, 0)
        except socket.timeout:
            self.send_error_response(client_socket, 504, "Gateway Timeout", "timed out")
            self.log_request(client_address, cache_status, timestamp, request.start_line, 504, 0)
        except ConnectionRefusedError:
            self.send_error_response(client_socket, 502, "Bad Gateway", "connection refused")
            self.log_request(client_address, cache_status, timestamp, request.start_line, 502, 0)
        except Exception as e:
            print(f"HTTP request error: {e}")
            self.send_error_response(client_socket, 502, "Bad Gateway", "connection failed")
            self.log_request(client_address, cache_status, timestamp, request.start_line, 502, 0)
    
    def transform_request(self, request, path):
        """Transform client request for forwarding to server"""
        # Create new request line with origin-form
        request_line = f"{request.method} {path} {request.version}\r\n"
        
        # Transform headers
        headers = []
        for header_line in request.raw_headers:
            name, value = header_line.split(':', 1)
            name = name.strip().lower()
            value = value.strip()
            
            if name == 'proxy-connection':
                continue  # Remove proxy-connection
            elif name == 'connection':
                headers.append("Connection: close\r\n")  # Force close
            elif name == 'via':
                headers.append(f"Via: {value}, 1.1 z5643027\r\n")  # Append our via
            else:
                headers.append(f"{header_line}\r\n")
        
        # Add Connection: close if not present
        if not any('connection:' in h.lower() for h in headers):
            headers.append("Connection: close\r\n")
        
        # Add/append Via header if not present
        if not any('via:' in h.lower() for h in headers):
            headers.append("Via: 1.1 z5643027\r\n")
        
        # Construct complete request
        transformed = request_line + ''.join(headers) + "\r\n"
        if request.body:
            transformed += request.body.decode('utf-8', errors='ignore')
        
        return transformed.encode('utf-8')
    
    def transform_response(self, response, original_request):
        """Transform server response for forwarding to client"""
        # Determine connection header based on client preference
        connection_header = original_request.headers.get('connection', '').lower()
        proxy_connection_header = original_request.headers.get('proxy-connection', '').lower()
        
        keep_alive = True
        if connection_header == 'close' or proxy_connection_header == 'close':
            keep_alive = False
        
        # Create response
        response_line = f"{response.version} {response.status_code} {response.reason_phrase}\r\n"
        
        # Transform headers
        headers = []
        for header_line in response.raw_headers:
            name, value = header_line.split(':', 1)
            name = name.strip().lower()
            value = value.strip()
            
            if name == 'connection':
                if keep_alive:
                    headers.append("Connection: keep-alive\r\n")
                else:
                    headers.append("Connection: close\r\n")
            elif name == 'via':
                headers.append(f"Via: {value}, 1.1 z5643027\r\n")
            else:
                headers.append(f"{header_line}\r\n")
        
        # Add Connection header if not present
        if not any('connection:' in h.lower() for h in headers):
            if keep_alive:
                headers.append("Connection: keep-alive\r\n")
            else:
                headers.append("Connection: close\r\n")
        
        # Add Via header if not present
        if not any('via:' in h.lower() for h in headers):
            headers.append("Via: 1.1 z5643027\r\n")
        
        # Construct complete response
        transformed = response_line + ''.join(headers) + "\r\n"
        if response.body:
            transformed = transformed.encode('utf-8') + response.body
        else:
            transformed = transformed.encode('utf-8')
        
        return transformed
    
    def send_error_response(self, client_socket, status_code, reason_phrase, error_message):
        """Send error response to client"""
        body = f"<html><body><h1>Error {status_code}</h1><p>{error_message}</p></body></html>"
        response = f"HTTP/1.1 {status_code} {reason_phrase}\r\n"
        response += "Content-Type: text/html\r\n"
        response += f"Content-Length: {len(body)}\r\n"
        response += "Connection: close\r\n"
        response += "\r\n"
        response += body
        
        try:
            client_socket.send(response.encode())
        except:
            pass
    
    def is_self_loop(self, host, port):
        """Check if request is pointing to proxy itself"""
        try:
            # Get all possible addresses for the host
            host_ips = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
            
            # Check if any of them match our proxy
            for info in host_ips:
                ip = info[4][0]
                if (ip == '127.0.0.1' or ip == '::1') and port == self.port:
                    return True
                # Could add more sophisticated checks for actual IP addresses
            
            return False
        except:
            return False
    
    def log_request(self, client_address, cache_status, timestamp, request_line, status_code, body_length):
        """Log request in Common Log Format"""
        host = client_address[0]
        port = client_address[1]
        
        log_entry = f'{host} {port} {cache_status} [{timestamp}] "{request_line}" {status_code} {body_length}'
        print(log_entry)

def main():
    """Main function"""
    if len(sys.argv) != 5:
        print("Usage: python3 proxy.py <port> <timeout> <max_object_size> <max_cache_size>")
        sys.exit(1)
    
    try:
        port = int(sys.argv[1])
        timeout = int(sys.argv[2])
        max_object_size = int(sys.argv[3])
        max_cache_size = int(sys.argv[4])
        
        if timeout <= 0 or max_object_size <= 0 or max_cache_size < max_object_size:
            raise ValueError("Invalid parameter values")
        
        proxy = HTTPProxy(port, timeout, max_object_size, max_cache_size)
        proxy.start()
        
    except ValueError as e:
        print(f"Error: Invalid command line arguments - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()