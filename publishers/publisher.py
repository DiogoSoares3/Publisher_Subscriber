import socket
import json
import time
import random


def main(config_file):
    with open(config_file, "r") as f:
        config = json.load(f)

    broker_address = config["broker_address"]
    broker_port = config["broker_port"]
    comments = config["comments"]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as pub_socket:
        pub_socket.connect((broker_address, broker_port))
        pub_socket.sendall(b"publisher")
        time.sleep(0.001) # Preventing broker's recv to concatenate strings from the publisher's first and second "sendall"
        
        for comment in comments:
            print("Sending comment to the broker!")
            pub_socket.sendall(json.dumps(comment).encode('utf-8'))
            time.sleep(random.randint(1, 2))


if __name__ == "__main__":
    import sys
    main(sys.argv[1])