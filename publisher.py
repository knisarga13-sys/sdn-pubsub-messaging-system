import socket
import json
import time
import sys

TOPIC_PORTS = {
    "sports": 5001,
    "weather": 5002,
    "news": 5003
}

# usage: python3 publisher.py <broker_ip> <topic> [count] [interval_sec]

broker_ip = sys.argv[1]
topic     = sys.argv[2]
count     = int(sys.argv[3]) if len(sys.argv) > 3 else 10  #no of messages to be sent 
interval  = float(sys.argv[4]) if len(sys.argv) > 4 else 1.0 #how many messages per second
port = TOPIC_PORTS[topic]

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((broker_ip, port)) #connects to the socket where broker recieves data
print(f"Connected to broker {broker_ip}:{port}, publishing to '{topic}'")

for seq in range(1, count + 1): #to send count number of messages
    msg = { #message format same as broker 
        "type": "PUBLISH",
        "topic": topic,
        "payload": f"message {seq} about {topic}",
        "seq": seq, #position of the message in order
        "ts": time.time(),     # send time, used for latency
    }
    sock.sendall((json.dumps(msg) + "\n").encode()) #puts everything in json format and sends to socket
    print(f"sent seq={seq}")
    time.sleep(interval) #sleep the interval between msgs
#repeats for every msg

sock.close() #closes socket after completion of transmission
print("Done.")
