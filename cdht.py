import sys
import socket
import time
import select

host = '127.0.0.1'
BASEPORT = 50000

# Global Setup
myid, succ1, succ2, pred1, pred2, timeout1, timeout2 = None, None, None, None, None, None, None
udp, tcp = None, None
pingTime = 10
seqNo1, seqNo2 = 0, 0
pingSeq1, pingSeq2 = [], []


'''
UDP and Ping Handler
'''
def UDPreceiver():
	global pred1, pred2, pingSeq1, pingSeq2
	data, address = udp.recvfrom(1024)
	dataParts = data.decode().split(',')
	# Ping response handle
	if len(dataParts) == 3:
		msg, seqNo, sender = dataParts
		if msg == 'pRes':
			print('A ping response message was received from Peer {}.'.format(sender))
			if succ1 == sender and int(seqNo) in pingSeq1:
				pingSeq1.remove(int(seqNo))
				pingSeq1 = [i for i in pingSeq1 if i > int(seqNo)] # Remove unanswered unconec pings
			elif succ2 == sender and int(seqNo) in pingSeq2: 
				pingSeq2.remove(int(seqNo))
				pingSeq2 = [i for i in pingSeq2 if i > int(seqNo2)]
	# Ping request handle
	elif len(dataParts) == 4:
		msg, seqNo, sender, pred = dataParts
		if msg == 'pReq':
			print('A ping request message was received from Peer {}.'.format(sender))
			sendUDP('pRes,{},{}'.format(seqNo, myid), sender)
			pred1 = sender if pred == '1' else pred1
			pred2 = sender if pred == '2' else pred2
	else:
		print('Corrupted data recevied:', data)


def sendUDP(msg, receiver):
	udp.sendto(msg.encode(), (host, BASEPORT+int(receiver)))


'''
TCP for file and successor updates
'''
def TCPreceiver():
	global succ1, succ2, pred1, pred2, pingSeq1, pingSeq2, seqNo1, seqNo2
	connection, address = tcp.accept()
	data = connection.recv(1024).decode()
	dataParts = data.split(',')
	# Quit message handle
	if data == 'q':
		print('Predecessors confimed to have received quit meesage. Exiting now.')
		sys.exit()
	# Successor update handle
	elif data == 'gSuc':
		connection.send(succ1.encode())
	# Peer leaving handle
	elif len(dataParts) == 4:
		msg, leaving_peer, new1, new2 = dataParts
		if msg == 'sUPD':
			print('Peer {} will depart from the network.'.format(leaving_peer))
			if leaving_peer == succ1:
				seqNo1, pingSeq1 = 0, []
			elif leaving_peer == succ2:
				seqNo2, pingSeq2 = 0, []
			succ1, succ2 = new1, new2
			print('My first successor is now peer {}.'.format(succ1))
			print('My second successor is now peer {}.'.format(succ2))
		elif msg == 'pUDP':
			pred1, pred2 = new1, new2
	# File request handle
	elif len(dataParts) == 3:
		msg, filename, requester = dataParts
		if msg == 'fReq':
			locateFile(filename, requester)
		elif msg == 'fRes':
			print('Received a response message from peer {}, which has the file {}.'.format(requester, filename))
	else:
		print('Corrupted data recevied:', data)
	connection.close()


def sendTCP(msg, receiver):
	try:
		tcpSend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		tcpSend.connect((host, BASEPORT + int(receiver)))
		tcpSend.send(msg.encode())
		tcpSend.close()
		return True
	except:
		return False


def locateFile(filename, requester):
	hash_val = (int(filename))%256
	# 3 cases:
	# 	1. Hash value between my id and my pred1
	# 	2. Pred has a larger value than myid. Hash value is larger than pred1
	# 	3. Hashvalue is between 1 to my id and my pred has a larger value
	if (hash_val <= int(myid) and hash_val > int(pred1)) or (int(myid) < int(pred1) and (hash_val > int(pred1) or hash_val <= int(myid))):
		print('File {} is here.'.format(filename))
		if requester != myid:
			if sendTCP('fRes,{},{}'.format(filename, myid), requester):
				print('A response message, destined for peer {}, has been sent'.format(requester))
			else:
				print('An error has occured while sending response message destined for peer {}.'.format(requester))
	else:
		print('File {} is not stored here.'.format(filename))
		if sendTCP('fReq,{},{}'.format(filename, requester), succ1):
			print('File Request message has been forwarded to my successor.')
		else:
			print('An error has occured while forwarding file request message to my successor')


def pingCheck():
	global timeout1, timeout2, pingSeq1, pingSeq2, succ1, succ2, seqNo1, seqNo2
	# if 3 or more consecutive ping not answered, then the successor has being forced quit
	if len(pingSeq1) > 2:
		timeout1 = time.time() if not timeout1 else timeout1
		if time.time() - timeout1 > 5: # Wait for 5s before declare dead
			print('Peer {} is no longer alive.'.format(succ1))
			succ1 = succ2
			print('My first successor is now peer {}.'.format(succ1))
			ungracefulQuit(succ1)
			print('My second successor is now peer {}.'.format(succ2))
			pingSeq1, pingSeq2, seqNo1, seqNo2 = [], [], 0, 0
	else:
		timeout1 = None

	if len(pingSeq2) > 2:
		timeout2 = time.time() if not timeout2 else timeout2
		if time.time() - timeout2 > 5:
			print('Peer {} is no longer alive.'.format(succ2))
			print('My first successor is now {}.'.format(succ1))
			ungracefulQuit(succ1)
			print('My second successor is now peer {}.'.format(succ2))
			pingSeq2, seqNo2 = [], 0
	else:
		timeout2 = None

def ungracefulQuit(receiver):
	global succ2
	tcpQuit = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	tcpQuit.connect((host, BASEPORT+int(receiver)))
	tcpQuit.send('gSuc'.encode())
	succ2 = tcpQuit.recv(1024).decode()
	tcpQuit.close()


def main():
	global myid, succ1, succ2, tcp, udp, seqNo1, seqNo2, pingSeq1, pingSeq2

	# Setup
	myid, succ1, succ2 = sys.argv[1], sys.argv[2], sys.argv[3]
	print('Initalised Peer {}. Successor 1 is {}, successor 2 is {}'.format(myid, succ1, succ2))
	udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	udp.bind((host, BASEPORT + int(myid)))
	udp.setblocking(0)
	tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	tcp.bind((host, BASEPORT + int(myid)))
	tcp.listen(5)
	tcp.setblocking(0)

	# Start loop
	while True:
		if select.select([sys.stdin,], [], [], 0.0)[0]: # Select module Docs: https://pymotw.com/2/select/  made modification so that it read from stdin instead and set timeout to 0.0
			line = sys.stdin.readline().rstrip()
			if line == 'quit':
				if (sendTCP('sUPD,{},{},{}'.format(myid, succ1, succ2), pred1) and 
					sendTCP('sUPD,{},{},{}'.format(myid, pred1, succ1), pred2) and 
					sendTCP('pUDP,{},{},{}'.format(myid, pred1, pred2), succ1) and 
					sendTCP('pUDP,{},{},{}'.format(myid, succ1, pred1), succ2)):
					sendTCP('q', myid)
				else:
					print('An error has occured while updating successors or predecessors')
			else:
				lineParts = line.split(' ')
				if len(lineParts) == 2:
					reqType, filename = lineParts
					if reqType == 'request':
						if filename.isdigit() and len(filename) == 4:
							locateFile(filename, myid)
						else:
							print('Invalid filename: {}'.format(filename))
					else:
						print('Invalid command: {}'.format(line))
				else:
					print('Invalid command: {}'.format(line))
		
		# read from tcp and udp socket # Select module Docs: https://pymotw.com/2/select/  made modification so that it read from tcp and udp instead and set timeout to 0.0
		sockets, _, _ = select.select([tcp, udp], [], [], 0.0)
		for sock in sockets:
			if sock == udp:
				UDPreceiver()
			elif sock == tcp:
				TCPreceiver()

		# send ping periodically
		if not int(time.time())%pingTime:
			sendUDP('pReq,{},{},{}'.format(seqNo1, myid, 1), succ1)
			pingSeq1.append(seqNo1)
			seqNo1 += 1
			sendUDP('pReq,{},{},{}'.format(seqNo2, myid, 2), succ2)
			pingSeq2.append(seqNo2)
			seqNo2 += 1
			time.sleep(1)

		# Check if any timeout
		pingCheck()


if __name__ == '__main__':
	main()
