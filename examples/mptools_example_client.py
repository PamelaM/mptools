import socket
import sys

ADDRESS = ('127.0.0.1', 9999)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(ADDRESS)
print(f"Connected to {ADDRESS}")
request = f"REQUEST {'::'.join(sys.argv[1:])}"
print("Sending : ", request)
sock.send(request.encode("utf-8"))

print("Receiving : ")
reply = sock.recv(1500).decode()
print("Received  : ", reply)

print("Closing socket")
sock.close()
