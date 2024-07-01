import socket
import json
from threading import Thread, Lock
from collections import defaultdict


TOPIC_QUEUES = defaultdict(list)
SUBSCRIBER_HISTORY = defaultdict(lambda: defaultdict(int))
LOCKER = Lock()

def handle_sub_topic(addr, topic):
    with LOCKER:
        print(f"Handling topic '{topic}' for subscriber {addr}")
        if topic in TOPIC_QUEUES:
            subscriber_index = SUBSCRIBER_HISTORY[addr].get(topic, 0) # Getting the last index read by this subscriber on this specific topic. If it's the first time the subscriber is reading the topic, sets index equal to 0.
            if subscriber_index < len(TOPIC_QUEUES[topic]): # If a publisher added more elements in this topic list, the subscriber index history will be smaller than the lenght of it.
                updates = list(TOPIC_QUEUES[topic])[subscriber_index:] # Get all new elements that were add on the topic's list.
                SUBSCRIBER_HISTORY[addr][topic] = len(TOPIC_QUEUES[topic]) # Updates the subscriber history on that specific topic.
                return updates
            return ["Nothing New!"]
        return ["No such topic!"]


def handle_pub_topic(comment):
    topic = comment["topic"]
    content = comment["content"]

    with LOCKER:
        if topic not in TOPIC_QUEUES: # If the topic doesn't exist, create an empty list
            TOPIC_QUEUES[topic] = []

        TOPIC_QUEUES[topic].append(content) # Add the a new element in the list


def handle_subscriber(sub_socket, addr):
    try:
        while True:
            topic = sub_socket.recv(1024).decode().strip() # Receive the topic signed by the subscriber

            if not topic:
                break

            print(f"Subscriber requested updates for topic: {topic}")
            updates = handle_sub_topic(addr, topic) # Get the updates

            sub_socket.sendall(json.dumps(updates).encode('utf-8') + b'\n') # Send response back

    except Exception as e:
        print(f"Error handling subscriber: {e}")

    finally:
        sub_socket.close()


def handle_publisher(pub_socket):    
    try:
        while True:
            comment = pub_socket.recv(4096).decode("utf-8") # Receive the comment publihed

            if not comment:
                break

            handle_pub_topic(json.loads(comment)) # Handle the topic and the content of the comment sent by the publisher

    except Exception as e:
        print(f"Error handling publisher: {e}")

    finally:
        pub_socket.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ADDRESS, PORT))
    server.listen(5)
    print(f"Broker listening on {ADDRESS}:{PORT}")
    
    try:
        while True:
            client_socket, addr = server.accept() # Wait for conections from publishers or subscribers
            client_type = client_socket.recv(1024).decode().strip() # After a publisher or a subscriber connect, the first message sent is the client identification
            print(f"Client type: {client_type}")

            if client_type == "publisher": # If client is a publisher, handle publisher
                print(f"Accepted publisher connection from {addr}")
                p = Thread(target=handle_publisher, args=(client_socket,))
                p.start()
            elif client_type == "subscriber": # If client is a subscriber, handle subscriber
                print(f"Accepted subscriber connection from {addr}")
                p = Thread(target=handle_subscriber, args=(client_socket, addr,))
                p.start()

    except Exception as e:
        print(f"Error in main loop: {e}")
    finally:
        server.close()

if __name__ == "__main__":
    with open('broker/broker_config.json', 'r') as f:
        config = json.load(f)
        
    ADDRESS = config['address']
    PORT = config['port']
    
    main()