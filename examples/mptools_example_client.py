import socket
import sys
import time


def one_request(*args):
    ADDRESS = ('127.0.0.1', 9999)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ADDRESS)
    print(f"CLIENT: Connected to {ADDRESS}")
    request = f"REQUEST {'::'.join(args)}"
    print("CLIENT: Sending : ", request)
    sock.send(request.encode("utf-8"))

    print("CLIENT: Receiving : ")
    reply = sock.recv(1500).decode()
    print("CLIENT: Received  : ", reply)

    print("CLIENT: Closing socket")
    sock.close()


print(f"CLIENT: Starting")

if sys.argv[1] and sys.argv[1] == "AUTO":
    for i in range(3):
        print(f"CLIENT: Waiting 5 seconds")
        time.sleep(5)
        one_request(f"TEST REQUEST {i + 1}")
    print(f"CLIENT: Waiting 5 seconds")
    time.sleep(5)
    one_request("END")
else:
    one_request(sys.argv[1:])

print(f"CLIENT: Exiting")
