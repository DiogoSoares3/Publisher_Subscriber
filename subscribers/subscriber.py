import socket
import json
import time
import random
import signal
import sys

# Define a global variable for the socket to ensure it can be closed properly
sub_socket = None

def signal_handler(sig, frame):
    global sub_socket
    if sub_socket:
        sub_socket.close()
    print('Subscriber socket closed. Exiting.')
    sys.exit(0)

def main(config_file):
    global sub_socket
    with open(config_file, "r") as f:
        config = json.load(f)

    broker_address = config["broker_address"]
    subscriber_address = config["subscriber_address"]
    signed_topics = config["signed_topics"]
    
    sub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Set SO_REUSEADDR option
    sub_socket.bind((subscriber_address["address"], subscriber_address["port"]))
    
    try:
        sub_socket.connect((broker_address["address"], broker_address["port"]))
        sub_socket.sendall(b"subscriber") # Sending client type
        time.sleep(0.001)  # Preventing broker's recv to concatenate strings from the publisher's first and second "sendall"
        
        for topic in signed_topics:
            print("Requesting updates on broker topics!")
            sub_socket.sendall(topic.encode())
            response = sub_socket.recv(4096).decode()
            if not response:
                break

            update = json.loads(response)
            print(f"Update on topic {topic}: ", update)
            time.sleep(random.randint(1, 2))
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        sub_socket.close()

if __name__ == "__main__":
    # Register the signal handler to close the socket properly on interruption
    signal.signal(signal.SIGINT, signal_handler)
    import sys
    main(sys.argv[1])