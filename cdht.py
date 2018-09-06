import socket, sys, threading, time

preId=0
pPreId=0
id = int(sys.argv[1])
sucId = int(sys.argv[2])
sSucId = int(sys.argv[3]) #second successor id
sucDectector = []
sSucDectector = []

def pingRecv():
    global id

    while True:
        global preId
        global pPreId
        global sucDectector
        global sSucDectector
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', id+50000))
        data, addr = sock.recvfrom(1024)
        sock.close()
        if data.split()[2] == "Request": #received ping request
            if data.split()[0] == "Successor":
                preId = int(data.split()[-1])
                sendBackSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                response = "Successor Ping Response from " + str(id)
                sendBackSock.sendto(response, ('', 50000+int(data.split()[-1])))
            elif data.split()[0] == "SSuccessor":
                pPreId = int(data.split()[-1])
                sendBackSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                response = "SSuccessor Ping Response from " + str(id)
                sendBackSock.sendto(response, ('', 50000+int(data.split()[-1])))

            print("A ping request message was received from Peer " + data.split()[-1] + ".")

            sendBackSock.close()



        else:  #receive ping response
            if data.split()[0] == "Successor":
                sucDectector[:] = []
                #print "S poped"
            elif data.split()[0] == "SSuccessor":
                sSucDectector[:] = []
                #print "SS poped"

            print("A ping response message was received from Peer " + data.split()[-1] + ".")


def pingSend():
    global id
    i = 0
    sucRequest = "Successor Ping Request from " + str(id)
    sSucRequest = "SSuccessor Ping Request from " + str(id)
    while True:
        global sucId
        global sSucId
        global sucDectector
        global sSucDectector

        #When suc was detected no longer alive, update sSuc as new suc and ask suc(newSuc) for newSSuc by using TCP
        if len(sucDectector) > 3:
            print("Peer " + str(sucId)+" is no longer alive.")
            sucId = sSucId #newSuc
            print("My first successor is now peer " + str(sucId) + ".")
            sucQuery = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #ask newSuc for newSSUC
            sucQuery.connect(('', sucId+50000))
            queryMessage = "Query"
            sucQuery.send(queryMessage)
            newsSucId = int(sucQuery.recv(1024))
            sSucId = newsSucId
            print("My second successor is now peer " + str(sSucId) + ".")
            sucQuery.close()
            sucDectector[:] = []

        sucSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sucSock.sendto(sucRequest, ('', sucId+50000))
        sucDectector.append(i)
        sucSock.close()
        time.sleep(5)

        #When sSuc was detected no longer alive, ask suc for a new sSuc by using TCP
        if len(sSucDectector) > 4:
            print("Peer " + str(sSucId)+" is no longer alive.")
            sSucQuery = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sSucQuery.connect(('', sucId+50000))
            queryMessage = "Query"
            sSucQuery.send(queryMessage)
            newSSucId = int(sSucQuery.recv(1024))
            sSucId = newSSucId
            print("My first successor is now peer " + str(sucId) + ".")
            print("My second successor is now peer " + str(sSucId) + ".")
            sSucQuery.close()
            sSucDectector[:] = []

        sSucSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)   
        sSucSock.sendto(sSucRequest, ('', sSucId+50000))
        sSucDectector.append(i)
        sSucSock.close()
        time.sleep(10)

        i = i + 1


# message will be in the format "Request {fileName} via {presuccessor} from {sourceRequester}
#                            or "Response {fileName} from {fileLocation}"
#                            or "sDepature"
#                            or "sSDepature"
def tcpRecv():
    global id

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', id+50000))
    sock.listen(5)
    while True:
        global sucId
        global sSucId
        con, addr = sock.accept()
        message = con.recv(1024)
        if message.split()[0] == "Request":  #if tcp socket received a file request message:
            fileName = int(message.split()[1])
            preId = int(message.split()[3]) # presuccessor id
            sourceId = int(message.split()[-1])
            hashVal = fileName % 256
            if id < hashVal and preId < id:  #if this peer doesn't have the requested file
                printMessage = "File " + str(fileName) + " is not here."
                print(printMessage)
                print "File request message has been forward to my successor."
                requestMessage = "Request " + str(fileName) + " via " + str(id) + " from " + str(sourceId)
                sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sendSock.connect(('', sucId+50000))
                sendSock.send(requestMessage)
                sendSock.close()
            else:       #if this peer has the requested file
                printMessage = "File " + str(fileName) + " is here."
                print(printMessage)
                printMessage = "A response message, destined for peer " + str(sourceId) + ", has been sent."
                print(printMessage)
                fileSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                fileSock.connect(('', sourceId+50000))
                fileMessage = "Response " + str(fileName) + " from " + str(id) 
                fileSock.send(fileMessage)
                fileSock.close()

        elif message.split()[0] == "Response":  #if tcp socket received a file response message
            fileLocationId = int(message.split()[-1])
            fileName = int(message.split()[1])
            printMessage = "Received a response message from peer " + str(fileLocationId) + ", which has the file " + str(fileName) +"."
            print(printMessage)

        elif message.split()[0] == "sDepature":
            sucId = int(message.split()[-2])
            sucDectector[:] = []
            sSucId = int(message.split()[-1])
            sSucDectector[:] = []
            sucMessage = "My first successor is now peer " + str(sucId) + "."
            sSucMessage = "My second successor is now peer " + str(sSucId) + "."
            print(sucMessage)
            print(sSucMessage)

        elif message.split()[0] == "sSDepature":
            sSucId = int(message.split()[-1])
            sSucDectector[:] = []
            sucMessage = "My first successor is now peer " + str(sucId) + "."
            sSucMessage = "My second successor is now peer " + str(sSucId) + "."
            print(sucMessage)
            print(sSucMessage)

        elif message == "Query":
            
            con.send(str(sucId))

        con.close()




def main():
    global id
    global sucId
    global sSucId
    global preId
    global pPreId

    pingRecvThread = threading.Thread(target = pingRecv)
    pingRecvThread.daemon = True
    pingRecvThread.start()
    time.sleep(0.1)
    pingSendThread = threading.Thread(target = pingSend)
    pingSendThread.daemon = True
    pingSendThread.start()
    time.sleep(0.1)
    tcpRecvThread = threading.Thread(target = tcpRecv)
    tcpRecvThread.daemon = True
    tcpRecvThread.start()



    while True:

        command = raw_input()
        if command.split()[0] == "request": #Request file from suc by using TCP
            requestMessage = "Request " + command.split()[1] + " via " + str(id) + " from " + str(id)
            requestSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            requestSock.connect(('', sucId+50000))
            requestSock.send(requestMessage)
            requestSock.close()
            printMessage = "File request message for " + str(command.split()[1] + " has been sent to my successor.")
            print(printMessage)

        elif command == "quit":# Send sucId and sSucId to presuc. Send sucId to pPreSuc by using TCP
            print("Peer " + str(id) + " will depart from network.")
            preSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            preSock.connect(('', preId+50000))
            depatureMessage = "sDepature " + str(id) + " suc and sSuc are " + str(sucId) + " " + str(sSucId)
            preSock.send(depatureMessage)
            preSock.close()
            pPreSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pPreSock.connect(('', pPreId+50000))
            depatureMessage = "sSDepature " + str(id) + "suc is " + str(sucId)
            pPreSock.send(depatureMessage)
            pPreSock.close()
            sys.exit()



        time.sleep(1)







if __name__ == "__main__":
    main()

























