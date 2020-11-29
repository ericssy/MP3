import socket               # Import socket module
import json
import pickle
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
host = socket.gethostname() # Get local machine name
port = 12346                # Reserve a port for your service.
s.bind((host, port))        # Bind to the port

# f = open('torecv.png','wb')
# s.listen(5)                 # Now wait for client connection.
while True:
    # l = json.loads(s.recvfrom(2048))
    data, server = s.recvfrom(2048)
    l = json.loads(data.decode('utf-8'))
    # l = json.loads(s.recvfrom(2048)[0].strip())
    print(l)
    # c, addr = s.accept()     # Establish connection with client.
    # print('Got connection from', addr)
    # print("Receiving...")
    # l = c.recv(1024)
    # l = pickle.loads(l)
    #
    # l = s.recv(1024)
    # l = l.decode("utf-8")


    # print(sock.recvfrom(2048)[0].strip())

    if (json.load(l)["type"] == "file"):
        f = open(json.load(l)["message"], 'wb')

        while (l):
            print("Receiving...")
            f.write(l)
            l = c.recv(1024)
        f.close()
        print("Done Receiving")
        c.send('Thank you for connecting')
        c.close()                # Close the connection