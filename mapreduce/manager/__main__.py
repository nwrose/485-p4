"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import threading
import pathlib
import shutil
from mapreduce.utils import tcp_server, udp_server, tcp_client, udp_client
from queue import Queue

# Configure logging
LOGGER = logging.getLogger(__name__)

# Configure project path root
PROJECT_ROOT = pathlib.Path().resolve()

class Manager:
    """Represent a MapReduce framework Manager node."""

    # includes values that need to be shared shared across threads
    signals = {"shutdown": 0}

    # Array of RemoteWorker objects
    workers = []

    # Dictionary mapping worker port to array index  {key=port, val=index}
    # this is useful for ordering and for when workers die and re-register
    # probably a more efficient way to do this, like OrderedDict but whatevs
    workers_order = {}

    # Job ID (UID so must increment)
    job_id = 0

    # Current Task list (index=taskid, value=status)
    current_tasks = []

    # Job Queue containing job JSON dicts
    job_queue = Queue()

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        #This is the heartbeat UDP listening thread
        udp_thread = threading.Thread(target=udp_server, args=(host, port, self.signals, self.handle_heartbeat))
        udp_thread.start()

        # This is the TCP listening thread
        tcp_thread = threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_msg))
        tcp_thread.start()
        
        # This is the fault tolerance thread
        fault_thread = threading.Thread(target=self.fault_tolerance, args=())
        fault_thread.start()
        time.sleep(1)
 
        # Main thread (this one) does the job handling
        while self.signals["shutdown"] == 0:
            time.sleep(0.5)
            if not self.job_queue.empty():
                job_to_do = self.job_queue.get()
                path_out = job_to_do.output_directory
                if os.path.exists(path_out):
                    shutil.rmtree(path_out)
                os.mkdir(path_out)
                prefix = f"mapreduce-shared-job{job_to_do.job_id:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    
                    # Do the input partitioning
                    input_list = os.listdir(job_to_do.input_directory)
                    # Create a list of absolute paths by joining the directory path with each filename
                    absolute_paths = [os.path.join(job_to_do.input_directory, file_name) for file_name in input_list]
                    input_list = absolute_paths
                    input_list.sort()    
                    parts = []
                    for i in range(job_to_do.num_mappers):
                        parts.append([])
                    for i in range(len(input_list)):
                        parts[i % job_to_do.num_mappers].append(input_list[i])
                    for i in range(len(parts)):
                        self.current_tasks.append("incomplete_map")
                        task_msg = {
                            "message_type": "new_map_task",
                            "task_id": i,
                            "input_paths": parts[i],
                            "executable": job_to_do.mapper_executable,
                            "output_directory": tmpdir,
                            "num_partitions": job_to_do.num_reducers
                        }
                        self.send_task(task_msg, job_to_do.job_id)
                    # Wait around until map job is done or gets shutdown order 
                    map_done = False
                    while self.signals["shutdown"] == 0 and not map_done:
                        #check if map job is done
                        map_done = True
                        for task in self.current_tasks:
                            if task != "complete":
                                map_done = False
                                break
                        time.sleep(0.2)
                    # Map Job is done
                    for i, task in enumerate(self.current_tasks):
                        task = "incomplete_reduce"
                        matching_files = pathlib.Path(tmpdir).glob(f"maptask?????-part{i:05d}")
                        reduce_input_files = []
                        for file in matching_files:
                            reduce_input_files.append(str(file))
                        print(matching_files)
                        task_msg = {
                            "message_type": "new_reduce_task",
                            "task_id": i,
                            "input_paths": reduce_input_files,
                            "executable": job_to_do.reducer_executable,
                            "output_directory": job_to_do.output_directory
                        }
                        self.send_task(task_msg, job_to_do.job_id)
                    reduce_done = False
                    while self.signals["shutdown"] == 0 and not reduce_done:
                        reduce_done = False
                        for task in self.current_tasks:
                            if task != "complete":
                                reduce_done = False
                                break
                        time.sleep(0.2)
                    # Reduce Job is done
                # Cleaning up tmpdir and check for new job
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
        # Keep checking for jobs until shutdown message hits

        # Wait until threads are closed to shutdown
        udp_thread.join()
        tcp_thread.join()
        fault_thread.join()

        pass # return?


    def send_task(self, task_msg, job_id):
        job_sent = False
        while not job_sent and self.signals["shutdown"] == 0:
            for worker in self.workers:
                if worker.status == "ready":
                    try:
                        print("ATTEMPTING TO SEND TASK")
                        tcp_client(worker.host, worker.port, task_msg)
                    except ConnectionRefusedError:
                        print("CONNECTION REFUSED")
                        self.dead_worker(worker)
                    else:
                        # Message successfully sent
                        print("MESSAGE SENT SUCCESSFULLY")
                        worker.assign_task(task_msg, job_id)
                        job_sent = True
                        break
            time.sleep(0.5)



    def fault_tolerance(self):
        """Ensure that workers have not died."""
        # time.sleep(0.5)
        while self.signals["shutdown"] == 0:
            time.sleep(2)
            for worker in self.workers:
                worker.missed_pings += 1
                if worker.missed_pings >= 5:
                    self.dead_worker(worker)

    def dead_worker(self, worker):
        """Handle a worker that has died."""
        worker.status = "dead"
        pass # IMPLIMENT ME


    def handle_heartbeat(self, heartbeat_msg):
        """Handle the heartbeat message."""
        key = (heartbeat_msg["worker_host"], heartbeat_msg["worker_port"])
        if key in self.workers_order:
            index = self.workers_order[key]
            self.workers[index].missed_pings = 0
            

    def handle_msg(self, msg):
        """Handle a TCP message to the manager."""
        if msg["message_type"] == "shutdown":
            self.initiate_shutdown()
        elif msg["message_type"] == "register":
            worker = {
                "host": msg["worker_host"],
                "port": msg["worker_port"],
            }
            key = (worker["host"], worker["port"])
            if key in self.workers_order:
                self.re_register(worker)
                print("RE REGISTERING WORKER")
            else:
                new_worker = self.RemoteWorker(worker["host"], worker["port"])
                self.workers_order[key] = len(self.workers)
                self.workers.append(new_worker)
                print("REGISTERING A WORKER")
                ack = {"message_type": "register_ack"}
                try:
                    tcp_client(worker["host"], worker["port"], ack)
                except ConnectionRefusedError:
                    self.dead_worker(new_worker)
                # IMPLEMENT assign new worker to job...
        elif msg["message_type"] == "new_manager_job":
            new_job = self.Job(
                self.job_id, msg["input_directory"], msg["output_directory"],
                  msg["mapper_executable"], msg["reducer_executable"],
                    msg["num_mappers"], msg["num_reducers"]
                    )
            self.job_queue.put(new_job)
            self.job_id += 1
        elif msg["message_type"] == "finished":
            self.finish_task(msg["task_id"], msg["worker_host"], msg["worker_port"])
        else:
            print("bad message_type sent to manager!  -->  ", msg["message_type"])
            pass


    def finish_task(self, task_id, worker_host, worker_port):
        key = (worker_host, worker_port)
        worker_index = self.workers_order[key]
        worker = self.workers[worker_index]
        worker.finish_task(task_id)
        self.current_tasks[task_id] = "complete"



    def re_register(self, worker):
        """Handle cases of a worker trying to re-register."""
        pass # IMPLIMENT ME

    def initiate_shutdown(self):
        '''Shut down all workers and then the manager.'''
        # Send shutdown messages to all workers
        shutdown_msg = {"message_type": "shutdown"}
        for worker in self.workers:
            if worker.status != "dead":
                tcp_client(worker.host, worker.port, shutdown_msg)
        # Shut down this manager (will cause exit listening loops in threads)
        print("shutting down manager")
        self.signals["shutdown"] = 1


    # Sub-class RemoteWorker of Manager
    class RemoteWorker:
        host = None
        port = None
        missed_pings = None

        # status = {"ready", "busy", "dead"}
        status = None

        #Dict example{
        # "message_type": "new_map_task" or "new_reduce_task",
        # "task_id": int,
        # "input_paths": list,
        # "executable": string,
        # "output_directory": string,
        # "num_partitions": int
        # }
        task = None
        current_job_id = None
       
        def __init__(self, host, port):
            self.host = host
            self.port = port
            self.status = "ready"
            self.missed_pings = 0


        def assign_task(self, task_msg, job_id):
            """Assign task to this Worker."""
            self.task = task_msg
            self.status = "busy"
            self.job_id = job_id

        def finish_task(self, task_id):
            if task_id != self.task["task_id"]:
                print("\n\nRecieved message for wrong task!!!!!\n\n")
            else:

                self.task = None
                self.job_id = None
                self.status = "ready"


        def unassign_task(self, task):
            """Unassign task and return it, e.g., when Worker is marked dead."""
            pass
    # end class RemoteWorker

    # Sub-class Job of Manager
    class Job:
        job_id = None
        input_directory = None
        output_directory = None
        mapper_executable = None
        reducer_executable = None
        num_mappers = None
        num_reducers = None

        def __init__(self, job_id, input_directory, output_directory, mapper_executable, reducer_executable, num_mappers, num_reducers):
            # Info for a Job
            self.job_id = job_id
            self.input_directory = input_directory
            self.output_directory = output_directory
            self.mapper_executable = mapper_executable
            self.reducer_executable = reducer_executable
            self.num_mappers = num_mappers
            self.num_reducers = num_reducers
            

        def next_task(self):
            """Return the next pending task to be assigned to a Worker."""
            pass


        def task_reset(self, task):
            """Re-enqueue a pending task, e.g., when a Worker is marked dead."""
            pass


        def task_finished(self, task):
            """Mark a pending task as completed."""
            pass

    # end class Job
# end class Manager

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
