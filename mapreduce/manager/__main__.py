"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import threading
import pathlib
import shutil
from queue import Queue
import click
from mapreduce.utils import tcp_server, udp_server, tcp_client

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
        self.signals = {"shutdown": 0}
        self.workers = []
        self.workers_order = {}
        self.job_id = 0
        self.current_tasks = []
        self.job_queue = Queue()

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

        # This is the heartbeat UDP listening thread
        udp_thread = threading.Thread(
            target=udp_server,
            args=(host, port, self.signals, self.handle_heartbeat))
        udp_thread.start()

        # This is the TCP listening thread
        tcp_thread = threading.Thread(
            target=tcp_server,
            args=(host, port, self.signals, self.handle_msg))
        tcp_thread.start()

        # This is the fault tolerance thread
        fault_thread = threading.Thread(target=self.fault_tolerance, args=())
        fault_thread.start()
        time.sleep(1)

        # This is the main thread
        self.main_thread()

        # Wait until threads are closed to shutdown
        udp_thread.join()
        tcp_thread.join()
        fault_thread.join()

    def main_thread(self):
        """Run the main thread."""
        # Main thread (this one) does the job handling
        while self.signals["shutdown"] == 0:
            time.sleep(0.1)
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
                    # Create a list of absolute paths by joining
                    absolute_paths = [os.path.join(
                        job_to_do.input_directory, file_name)
                            for file_name in input_list]
                    input_list = absolute_paths
                    input_list.sort()
                    parts = []
                    for i in range(job_to_do.num_mappers):
                        parts.append([])
                    for i, list_item in enumerate(input_list):
                        parts[i % job_to_do.num_mappers].append(list_item)
                    # Send the map job helper function
                    self.send_map_job(parts, tmpdir, job_to_do)
                    # Wait around until map job is done or gets shutdown order
                    map_done = False
                    while self.signals["shutdown"] == 0 and not map_done:
                        # check if map job is done
                        map_done = self.check_map_done()
                        time.sleep(0.5)
                    # Map Job is done
                    self.send_reduce_job(parts, tmpdir, job_to_do)
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

    def check_map_done(self):
        """Check if the map tasks are all complete."""
        map_done = True
        for task in self.current_tasks:
            if task != "complete":
                map_done = False
                break
        return map_done

    def send_map_job(self, parts, tmpdir, job_to_do):
        """Send a map job."""
        for i, part in enumerate(parts):
            self.current_tasks.append("incomplete_map")
            task_msg = {
                "message_type": "new_map_task",
                "task_id": i,
                "input_paths": part,
                "executable": job_to_do.mapper_executable,
                "output_directory": tmpdir,
                "num_partitions": job_to_do.num_reducers
            }
            self.send_task(task_msg, job_to_do.job_id)

    def send_reduce_job(self, parts, tmpdir, job_to_do):
        """Send a reduce job."""
        for i, task in enumerate(self.current_tasks):
            print(task)
            task = "incomplete_reduce"
            matching_files = pathlib.Path(tmpdir).glob(
                f"maptask?????-part{i:05d}")
            parts = []
            for file in matching_files:
                parts.append(str(file))
            task_msg = {
                "message_type": "new_reduce_task",
                "task_id": i,
                "input_paths": parts,
                "executable": job_to_do.reducer_executable,
                "output_directory": job_to_do.output_directory
            }
            self.send_task(task_msg, job_to_do.job_id)

    def send_task(self, task_msg, job_id):
        """Send a task to a worker."""
        job_sent = False
        while not job_sent and self.signals["shutdown"] == 0:
            for worker in self.workers:
                if worker.status == "ready":
                    try:
                        tcp_client(worker.host, worker.port, task_msg)
                    except ConnectionRefusedError:
                        self.dead_worker(worker)
                        # in this path, task will continue to look for a worker
                    else:
                        # Message successfully sent
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
                if worker.status != "dead":
                    worker.missed_pings += 1
                    if worker.missed_pings >= 5:
                        self.dead_worker(worker)

    def dead_worker(self, worker):
        """Handle a worker that has died."""
        worker.status = "dead"
        dead_task = worker.task
        dead_job_id = worker.current_job_id
        worker.task = None
        worker.current_job_id = None
        self.send_task(dead_task, dead_job_id)

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
            else:
                new_worker = self.RemoteWorker(worker["host"], worker["port"])
                self.workers_order[key] = len(self.workers)
                self.workers.append(new_worker)
                ack = {"message_type": "register_ack"}
                try:
                    tcp_client(worker["host"], worker["port"], ack)
                except ConnectionRefusedError:
                    self.dead_worker(new_worker)
                # IMPLEMENT assign new worker to job...
        elif msg["message_type"] == "new_manager_job":
            new_job = self.Job(self.job_id, msg)
            self.job_queue.put(new_job)
            self.job_id += 1
        elif msg["message_type"] == "finished":
            self.finish_task(msg["task_id"],
                             msg["worker_host"],
                             msg["worker_port"])
        else:
            print("bad message_type sent to manager: ", msg["message_type"])

    def finish_task(self, task_id, worker_host, worker_port):
        """Handle the manager side of a finished task."""
        key = (worker_host, worker_port)
        worker_index = self.workers_order[key]
        worker = self.workers[worker_index]
        if worker.task["task_id"] == task_id:
            worker.finish_task(task_id)
            self.current_tasks[task_id] = "complete"

    def re_register(self, worker):
        """Handle cases of a worker trying to re-register."""
        self.dead_worker(worker)
        key = (worker.host, worker.port)
        new_worker = self.RemoteWorker(worker["host"], worker["port"])
        self.workers_order[key] = len(self.workers)
        self.workers.append(new_worker)
        ack = {"message_type": "register_ack"}
        try:
            tcp_client(worker["host"], worker["port"], ack)
        except ConnectionRefusedError:
            self.dead_worker(new_worker)
        # IMPLEMENT assign new worker to job...

    def initiate_shutdown(self):
        """Shut down all workers and then the manager."""
        # Send shutdown messages to all workers
        shutdown_msg = {"message_type": "shutdown"}
        for worker in self.workers:
            if worker.status != "dead":
                tcp_client(worker.host, worker.port, shutdown_msg)
        # Shut down this manager (will cause exit listening loops in threads)
        self.signals["shutdown"] = 1

    class RemoteWorker:
        """Represent a worker."""

        host = None
        port = None
        missed_pings = None

        # status = {"ready", "busy", "dead"}
        status = None

        # Dict example{
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
            """Initialize a Worker object."""
            self.host = host
            self.port = port
            self.status = "None"
            self.current_job_id = "None"
            self.status = "ready"
            self.missed_pings = 0

        def assign_task(self, task_msg, job_id):
            """Assign task to this Worker."""
            self.task = task_msg
            self.status = "busy"
            self.current_job_id = job_id

        def finish_task(self, task_id):
            """Finish task for this worker."""
            if task_id != self.task["task_id"]:
                print("\n\nRecieved message for wrong task!!!!!\n\n")
            else:
                self.task = None
                self.current_job_id = None
                self.status = "ready"
    # end class RemoteWorker

    # Sub-class Job of Manager
    class Job:
        """Represent a Job."""

        job_id = None
        input_directory = None
        output_directory = None
        mapper_executable = None
        reducer_executable = None
        num_mappers = None
        num_reducers = None

        def __init__(self, job_id, msg):
            """Initialize a Job object."""
            # Info for a Job
            self.job_id = job_id
            self.input_directory = msg["input_directory"]
            self.output_directory = msg["output_directory"]
            self.mapper_executable = msg["mapper_executable"]
            self.reducer_executable = msg["reducer_executable"]
            self.num_mappers = msg["num_mappers"]
            self.num_reducers = msg["num_reducers"]

        def set_job_id(self, job_id):
            """Set the job_id for this Job."""
            self.job_id = job_id
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
