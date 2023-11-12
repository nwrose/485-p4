"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import pathlib
import threading
import subprocess
import heapq
import hashlib
import shutil
import tempfile
from contextlib import ExitStack
from mapreduce.utils import tcp_server, udp_server, tcp_client, udp_client

# Configure logging
LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path().resolve()

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
            print("ATTEMPTING TO SHUT DOWN")
        elif msg["message_type"] == "register_ack":
            self.heartbeat_thread = threading.Thread(target=self.handle_heartbeat, args=())
            self.heartbeat_thread.start()
            time.sleep(1)
        elif msg["message_type"] == "new_map_task":
            self.map_files(msg)
        elif msg["message_type"] == "new_reduce_task":
            self.reduce_files(msg)
        else:
            print("bad message_type sent to worker! -->  ", msg["message_type"])

    # Message contains: task_id int, executable string, input_paths [string], output_directory string
    def reduce_files(self, msg):
        """Reduce step of mapreduce."""
        #Make the tempdir
        prefix = f"mapreduce-local-task{msg['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            file_path = f"part-{msg['task_id']:05d}"
            absolute_path = os.path.join(tmpdir, file_path)
            with open(absolute_path, 'a', encoding="utf-8") as outfile:
                with ExitStack() as stack:
                    # I NEVER GET HERE
                    infiles = [stack.enter_context(open(fname)) for fname in msg["input_paths"]]
                    merged_infiles = heapq.merge(*infiles)
                    with subprocess.Popen([msg["executable"]], stdin=subprocess.PIPE, stdout=outfile, text=True) as reduce_process:
                        for line in merged_infiles:
                            reduce_process.stdin.write(line)
                            # close the file
                        # all lines written
                    # subprocess is done
                # infiles and outfile have been used and can be closed
            shutil.move(absolute_path, msg["output_directory"])
        # tmpdir is no longer needed
        finished_msg = {
            "message_type": "finished",
            "task_id": msg["task_id"],
            "worker_host": self.host,
            "worker_port": self.port
        }
        tcp_client(self.manager_host, self.manager_port, finished_msg)


            
    # Message contains: task_id int, input_paths [string], executable string, output_directory string, num_partitions int
    def map_files(self, msg):
        """Map step of mapreduce."""
        # Make the tempdir
        prefix = f"mapreduce-local-task{msg['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            # Run .exe on input files and hash results into the tempdirs
            with ExitStack() as stack:
                infiles = [stack.enter_context(open(fname)) for fname in msg["input_paths"]]
                for infile in infiles:
                    with subprocess.Popen([msg["executable"]], stdin=infile, stdout=subprocess.PIPE, text=True) as map_process:
                        for line in map_process.stdout:
                            key = line.split('\t')[0]
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = keyhash % msg["num_partitions"]
                            file_path = f"maptask{msg['task_id']:05d}-part{partition_number:05d}"
                            absolute_path = os.path.join(tmpdir, file_path)
                            with open(absolute_path, 'a', encoding="utf-8") as file:
                                # Write the new line to the file
                                file.write(line)
            my_files = os.listdir(tmpdir)
            absolute_paths = [os.path.join(tmpdir, file_name) for file_name in my_files]
            my_files = absolute_paths
            for filename in my_files:
                subprocess.run(["sort", "-o", filename, filename], check=True)
                shutil.move(filename, msg["output_directory"])
        # Done with tmpdir
        finished_msg = {
            "message_type": "finished",
            "task_id": msg["task_id"],
            "worker_host": self.host,
            "worker_port": self.port
        }
        tcp_client(self.manager_host, self.manager_port, finished_msg)
        

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
