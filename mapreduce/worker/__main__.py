"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import threading
from mapreduce.utils import tcp_server, udp_server, tcp_client, udp_client

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    
    #signals
    signals = {"shutdown": 0}

    # Keep track of the host and port for this worker and its manager
    host = None
    port = None
    manager_host = None
    manager_port = None

    # Allow access of heartbeat thread from anywhere
    heartbeat_thread = None

    # Class constructor
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
        
        # Set the class variables
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.host = host
        self.port = port

        # Start a tcp listening thread and then tell manager "I'm ready"
        tcp_thread = threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_msg))
        tcp_thread.start()
        time.sleep(1)
        ready_msg = {
            "message_type": "register",
            "worker_host": host,
            "worker_port": port
        }
        tcp_client(manager_host, manager_port, ready_msg)

        #finish all threads before exiting
        tcp_thread.join()
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()
        print(f"shutting down worker @ host:{host} and port:{port}")


    def handle_msg(self, msg):
        """Handle a message sent to worker using TCP."""

        if msg["message_type"] == "shutdown":
            self.signals["shutdown"] = 1
        elif msg["message_type"] == "register_ack":
            self.heartbeat_thread = threading.Thread(target=self.handle_heartbeat, args=())
            self.heartbeat_thread.start()
            time.sleep(1)
        elif msg["message_type"] == "new_map_task":
            pass #IMPLIMENT ME
        elif msg["message_type"] == "new_reduce_task":
            pass #IMPLIMENT ME
        else:
            print("bad message_type sent to worker! -->  ", msg["message_type"])


    def handle_heartbeat(self):
        heartbeat_msg = {  
            "message_type": "heartbeat",
            "worker_host": self.host,
            "worker_port": self.port
        }
        while self.signals["shutdown"] == 0:
            time.sleep(2)
            udp_client(self.manager_host, self.manager_port, heartbeat_msg)



@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
