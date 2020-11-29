import socket               # Import socket module
import json
import pickle
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 12346                # Reserve a port for your service.
f = open('test1.txt','rb')
print('Sending...')
message = {"message": "test1.txt", "type": "file"}
s.sendto(json.dumps(message).encode('utf-8'), (host, port))

s.close


# s.sendall(bytes(json.dumps(message), encoding="utf-8"))
# l = f.read(1024)
# while (l):
#     print('Sending...')
#     s.send(l)
#     l = f.read(1024)
# f.close()
# print("Done Sending")
# print(s.recv(1024))

