import socket
import json
import time
import sys
import threading

TOPIC_PORTS = {
    "sports": 5001,
    "weather": 5002,
    "news": 5003
}

# usage: python3 subscriber.py <broker_ip> <topic>

broker_ip = sys.argv[1]
topic = sys.argv[2]
port = TOPIC_PORTS[topic]

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((broker_ip, port))


def send(obj):
    sock.sendall((json.dumps(obj) + "\n").encode())


def receiver():
    buf = ""
    while True:
        data = sock.recv(4096) #check the socket for 4096 bytes
        if not data: #if nothing is on socket
            print("Broker closed the connection.")
            break
        buf += data.decode() #put data into json format from binary
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            if not line.strip():
                continue
            msg = json.loads(line) #conversts json format to normal text
            if msg["type"] == "MESSAGE": #finds msg with type msg
                latency_ms = (time.time() - msg["ts"]) * 1000  #to measure the latency
                print(f"[{msg['topic']}] {msg['payload']}  (latency {latency_ms:.2f} ms)")
            elif msg["type"] == "ACK":  #checks if msg type is ACK
                print(f"broker: {msg['info']}")


threading.Thread(target=receiver, daemon=True).start() #runs the actual thread for this code

send({"type": "SUBSCRIBE", "topic": topic}) #subscribes to the topics mentioned in CLI
 #(check where topics is defined in this code )

print("Type 'sub <topic>' or 'unsub <topic>' to change subscriptions, 'quit' to exit.")
try:
    while True:
        cmd = input().strip().split() #insput
        if not cmd:
            continue
        if cmd[0] == "sub" and len(cmd) == 2: #if ur input is sub smth it will send sub for that topic
            send({"type": "SUBSCRIBE", "topic": cmd[1]})
        elif cmd[0] == "unsub" and len(cmd) == 2:
            send({"type": "UNSUBSCRIBE", "topic": cmd[1]})
        elif cmd[0] == "quit": #ends the inputs
            break
except (EOFError, KeyboardInterrupt):
    pass
sock.close()
