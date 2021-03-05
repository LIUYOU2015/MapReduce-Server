"""Master.

Define Master class.
"""
import os
import sys
import shutil
from pathlib import Path
import logging
import json
import time
import socket
import threading
import heapq
import click
from mapreduce.utils import tcp_send
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)


STATUS_IND = ['READY', 'BUSY', 'DEAD']


class TMPworker:
    """Data structure for Master storing worker information."""

    def __init__(self, port, worker_pid, output_id, status):
        """Init Worker as idle and empty job message."""
        self.args = {
            'port': port,
            'worker_pid': worker_pid,
            'output_id': output_id
        }
        self.status = status
        self.ping_missed = 0
        self.work_job_msg = []
        self.cur_work_job = None
        self.pending = 0

    def is_busy(self):
        """To test if the worker can be set 'READY'."""
        return self.work_job_msg and self.cur_work_job

    def add_job_msg(self, new_job_msg):
        """Add job message to queue."""
        self.work_job_msg.append(new_job_msg)
        self.pending += 1
        if self.status == 'DEAD':
            return
        self.cur_work_job = self.work_job_msg.pop(0)
        tcp_send(self.cur_work_job, self.args['port'])
        self.status = 'BUSY'

    def finish_cur_job(self):
        """Received finish message from worker.

        check the work message queue,
        distribute next job or just be idle.
        """
        self.cur_work_job = None
        self.pending -= 1
        if not self.pending:
            self.status = 'READY'

    def clean_jobs(self):
        """Clean all jobs when finished."""
        self.work_job_msg = []
        self.cur_work_job = None
        self.pending = 0


class Job:
    """Class for Master storing job info."""

    def __init__(self, job_id, new_job_msg):
        """Init Job."""
        self.job_id = job_id
        self.job_msg = {
            'input_dir': new_job_msg['input_directory'],
            'out_dir': new_job_msg['output_directory'],
            'mapper_exe': new_job_msg['mapper_executable'],
            'reducer_exe': new_job_msg['reducer_executable'],
            'num_mappers': new_job_msg['num_mappers'],
            'num_reducers': new_job_msg['num_reducers']
        }
        self.stage = None
        self.workers = {}
        self.reschedule = []

    def add_to_reschedule(self, jobs):
        """Add jobs to reschedule list."""
        self.reschedule.extend(jobs)

    def need_to_reschedule(self):
        """Check whether jobs need to be rescheduled or not."""
        return len(self.reschedule) > 0

    def clean_reschedule(self):
        """Clean up reschedule list."""
        self.reschedule = []


class Master:
    """Master.

    listen for MapReduce jobs,
    manage the jobs,
    distribute work amongst the workers,
    handle faults.
    """

    def __init__(self, port):
        """Init Master, create sockets."""
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        # folder tmp: storing intermediate files used by the MapReduce server
        tmp_dir = Path('tmp')
        tmp_dir.mkdir(exist_ok=True)
        for prev_job in list(tmp_dir.glob('job-*')):
            shutil.rmtree(prev_job)

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('localhost', port))
        self.server.listen(10)
        self.server.settimeout(1)

        self.udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_server.bind(('localhost', port - 1))
        self.udp_server.settimeout(1)

        self.shutdown = False
        self.worker_map = {}
        self.job_queue = []
        self.cur_job = None
        self.job_counter = 0

        # thread will be ended if process is ended, when daemon is True
        thread_heart = threading.Thread(target=self.listen_heartbeat,
                                        daemon=True)
        thread_heart.start()

        thread_detect = threading.Thread(target=self.detect,
                                         daemon=True)
        thread_detect.start()

        # thread_data = threading.Thread(target=self.listen_worker)
        # thread_data.start()

        self.listen_worker()

    def listen_worker(self):
        """Listen shutdown message and message from workers."""
        logging.info('start')
        while not self.shutdown:
            logging.info('waiting')
            try:
                client, _ = self.server.accept()
                # logging.info(client)
            except socket.timeout:
                continue
            # logging.info('Worker accepted client')
            data = bytearray()
            msg = {}
            try:
                while True:
                    # logging.info('Client recving data...')
                    chunk = client.recv(4096)
                    if not chunk:
                        client.close()
                        # logging.info('Client finished recving data...')
                        break
                    data.extend(chunk)
            except OSError:
                continue

            try:
                msg = json.loads(data.decode('utf-8'))
                logging.info("Master received: %s", msg)
            except ValueError:
                continue

            if msg['message_type'] == 'register':
                worker = TMPworker(msg['worker_port'],
                                   msg['worker_pid'],
                                   '%02d' % (len(self.worker_map) + 1),
                                   'READY'
                                   )
                self.worker_map[msg['worker_pid']] = worker
                data = msg.copy()
                data['message_type'] = 'register_ack'
                tcp_send(data, msg['worker_port'])
                self.check_reschedule(worker)
            elif msg['message_type'] == "shutdown":
                data = {
                    "message_type": "shutdown",
                }
                for _, worker in self.worker_map.items():
                    if worker.status != "DEAD":
                        tcp_send(data, worker.args['port'])
                self.server.close()
                self.shutdown = True
                self.udp_server.close()
            elif msg['message_type'] == "new_master_job":
                # create tmp folders for new job
                base_dir = "tmp/job-" + str(self.job_counter)
                os.mkdir(base_dir)
                os.mkdir(base_dir + "/mapper-output")
                os.mkdir(base_dir + "/grouper-output")
                os.mkdir(base_dir + "/reducer-output")
                new_job = Job(self.job_counter, msg)
                self.job_queue.append(new_job)
                self.job_counter += 1
                # try to start new job
                self.start_new_job()
            elif msg["message_type"] == "status":
                self.handle_status_msg(msg)
        sys.exit(0)

    def handle_status_msg(self, status_msg):
        """Reveive worker's status messages."""
        # modify worker's status
        worker_pid = status_msg["worker_pid"]
        worker = self.cur_job.workers[worker_pid]
        worker.finish_cur_job()
        logging.info('Handle status msg ing...')

        self.check_reschedule(worker)

        # for _, worker in self.cur_job.workers.items():
        #     logging.info(worker.args['worker_pid'], worker.status)
        for _, worker in self.cur_job.workers.items():
            if worker.status == 'BUSY':
                logging.info('Handle status msg: Task not finish')
                return

        # if all workers have finished, enter next stage
        logging.info("Current Stage: %s", self.cur_job.stage)
        logging.info("ready for next stage")

        if self.cur_job.stage == 'MAP':
            self.cur_job.stage = 'GROUP'
            self.group()
        elif self.cur_job.stage == 'GROUP':
            self.cur_job.stage = 'REDUCE'
            self.reduce()
        elif self.cur_job.stage == 'REDUCE':
            self.cur_job.stage = 'WRAP'
            self.wrap()

    def start_new_job(self):
        """Start new job when possible."""
        if not self.job_queue or self.cur_job or not self.worker_map:
            return
        self.cur_job = self.job_queue.pop(0)
        self.cur_job.stage = "MAP"
        self.map()

    def map(self):
        """Phase Map in MapReduce."""
        # get workers for current job
        if self.num_living_workers() <= self.cur_job.job_msg['num_mappers']:
            self.cur_job.workers = \
                {pid: worker for pid, worker in self.worker_map.items()
                 if worker.status != 'DEAD'}
        else:
            i = 0
            for pid, worker in self.worker_map.items():
                if worker.status == 'DEAD':
                    continue
                self.cur_job.workers[pid] = worker
                i = i + 1
                if i == self.cur_job.job_msg['num_mappers']:
                    break
        job_parts = partition_job(
            self.cur_job.job_msg['input_dir'],
            os.listdir(self.cur_job.job_msg['input_dir']),
            self.cur_job.job_msg['num_mappers']
        )

        i = 0
        while 1:
            if i == self.cur_job.job_msg['num_mappers']:
                break
            for pid, worker in self.cur_job.workers.items():
                self.send_new_worker_job(worker, job_parts[i],
                                         "tmp/job-" + str(self.cur_job.job_id)
                                         + "/mapper-output",
                                         self.cur_job.job_msg['mapper_exe'])
                self.cur_job.workers[pid].status = 'BUSY'
                # logging.info("%d-th job: %s", i, job_parts[i])
                i = i + 1
                if i == self.cur_job.job_msg['num_mappers']:
                    break

        self.cur_job.stage = 'MAP'

    def group(self):
        """Phase Group in MapReduce."""
        base_dir = "tmp/job-" + str(self.cur_job.job_id)
        map_dir = base_dir + "/mapper-output"
        group_dir = base_dir + "/grouper-output"

        # https://eecs485staff.github.io/p4-mapreduce/#grouping-master--workers
        self.cur_job.workers = \
            {pid: worker for pid, worker in self.worker_map.items()
             if worker.status != 'DEAD'}
        job_parts = partition_job(map_dir,
                                  os.listdir(map_dir),
                                  len(self.cur_job.workers))
        i = 0
        for pid, worker in self.cur_job.workers.items():
            self.send_new_sort_job(
                worker, job_parts[i],
                os.path.join(group_dir, "sorted" + worker.args['output_id'])
            )
            self.cur_job.workers[pid].status = 'BUSY'
            i = i + 1

        self.cur_job.stage = 'GROUP'

    def merge(self):
        """Merge results after grouping."""
        base_dir = "tmp/job-" + str(self.cur_job.job_id)
        group_dir = base_dir + "/grouper-output"

        file_list = os.listdir(group_dir)
        sorted_files = list(map(
            lambda file: open(os.path.join(group_dir, file), "r"),
            file_list
        ))
        out = []
        # for file in file_list:
        #    sorted_files.append(open(os.path.join(group_dir, file), "r"))
        merged_file = heapq.merge(*sorted_files)

        for i in range(self.cur_job.job_msg['num_reducers']):
            reduce_fn = os.path.join(group_dir, "reduce" + '%02d' % (i + 1))
            out.append(open(reduce_fn, "w"))

        cur_str = next(merged_file)
        print(merged_file)
        cur_key = cur_str.split('\t')[0]
        key_num = 0
        while True:
            try:
                # get the next sorted line
                ind = key_num % (self.cur_job.job_msg['num_reducers'])
                while cur_str.split('\t')[0] == cur_key:
                    out[ind].write(cur_str)
                    cur_str = next(merged_file)
                key_num += 1
                cur_key = cur_str.split('\t')[0]
            except StopIteration:
                break

        for file in sorted_files:
            file.close()
        for file in out:
            file.close()

    def reduce(self):
        """Phase Reduce in MapReduce."""
        base_dir = "tmp/job-" + str(self.cur_job.job_id)
        group_dir = base_dir + "/grouper-output"
        reduce_dir = base_dir + "/reducer-output"

        # Merge
        self.merge()

        # start reduce stage
        # group_dir = base_dir + '/grouper-output/'
        if self.num_living_workers() <= self.cur_job.job_msg['num_reducers']:
            self.cur_job.workers = self.worker_map
        else:
            i = 0
            for pid, worker in self.worker_map.items():
                if worker.status == 'DEAD':
                    continue
                self.cur_job.workers[pid] = worker
                i = i + 1
                if i == self.cur_job.job_msg['num_reducers']:
                    break
        job_parts = partition_job(group_dir,
                                  [file for file in os.listdir(group_dir)
                                   if file.startswith("reduce")],
                                  self.cur_job.job_msg['num_reducers'])

        i = 0
        logging.info('start reduce')
        while 1:
            if i == self.cur_job.job_msg['num_reducers']:
                break
            for pid, worker in self.cur_job.workers.items():
                self.send_new_worker_job(worker, job_parts[i],
                                         reduce_dir,
                                         self.cur_job.job_msg['reducer_exe'])
                self.cur_job.workers[pid].status = 'BUSY'
                logging.info("%d-th job: %s", i, job_parts[i])
                i = i + 1
                if i == self.cur_job.job_msg['num_reducers']:
                    break

        self.cur_job.stage = 'REDUCE'

    def wrap(self):
        """Wrap up results after reducing."""
        base_dir = "tmp/job-" + str(self.cur_job.job_id)
        reduce_dir = base_dir + "/reducer-output"
        self.cur_job.stage = 'REDUCE'

        # finish currrent job
        file_list = os.listdir(reduce_dir)
        out_dir = self.cur_job.job_msg['out_dir']
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        file_list.sort()
        for filename in file_list:
            src = reduce_dir + '/' + filename
            dst = out_dir + '/outputfile' + filename[-2:]
            os.rename(src, dst)
        logging.info("Finish job %s", self.cur_job)
        self.cur_job = None
        self.start_new_job()

    def listen_heartbeat(self):
        """Listen shutdown message and message from workers."""
        logging.info('start listen to heartbeats')
        while True:
            # print('listen heartbeat time: {}'.format(t))
            try:
                # print('Listen heartbeat: recving...')
                data = self.udp_server.recv(4096)
                # print('Listen heartbeat: recv end.')
            except socket.timeout:
                # print('Listen heartbeat time out t = {}'.format(t))
                continue
            except OSError:
                # print('Listen heartbeat OS error!')
                continue

            # try:
            #    msg = json.loads(data.decode('utf-8'))
            #    logging.info("Master received: %s", msg)
            # except ValueError:
            #    continue
            msg = json.loads(data.decode('utf-8'))

            if msg['message_type'] == "heartbeat":
                # print("Received Heartbeat: {}".format(msg))
                pid = msg['worker_pid']
                if pid in self.worker_map:
                    self.worker_map[pid].ping_missed = 0

        logging.info("Stop listening heartbeats")

    def detect(self):
        """Detect dead worker."""
        while True:
            for worker in self.worker_map.values():
                if worker.status == 'DEAD' and worker.pending == 0:
                    continue
                worker.ping_missed += 1
                if worker.ping_missed >= 5:
                    dead_worker = worker
                    if dead_worker.status != 'DEAD':
                        dead_worker.status = 'DEAD'
                    if not self.cur_job:
                        return
                    logging.info('Worker %d is dead. Rescheduleing...',
                                 dead_worker.args['worker_pid'])
                    dead_worker_jobs = dead_worker.work_job_msg
                    if dead_worker.cur_work_job:
                        dead_worker_jobs = \
                            [worker.cur_work_job] + dead_worker_jobs

                    ready_workers = [w for w in self.worker_map.values()
                                     if w.status == 'READY']
                    num_ready_workers = len(ready_workers)
                    if num_ready_workers > 0:
                        self.reschedule(dead_worker_jobs, ready_workers)
                    else:
                        self.cur_job.add_to_reschedule(dead_worker_jobs)

                    dead_worker.clean_jobs()

            time.sleep(2)

    def check_reschedule(self, worker):
        """Check if we need to reschedule and do it if possible."""
        logging.info('Check reschedule...')
        if self.cur_job and self.cur_job.need_to_reschedule():
            logging.info('Need to reschedule.')
            if worker.status == 'READY':
                self.reschedule(self.cur_job.reschedule, [worker])

    def reschedule(self, jobs, workers):
        """Reschedule jobs to workers."""
        self.cur_job.clean_reschedule()
        num_workers = len(workers)

        for job in jobs:
            job_type = job['message_type']
            job_parts = [[] for _ in range(num_workers)]
            for i, input_file in enumerate(job['input_files']):
                job_parts[i % num_workers].append(input_file)

            for i, worker in enumerate(workers):
                self.cur_job.workers[worker.args['worker_pid']] = worker
                if job_type == 'new_worker_job':
                    if i == self.cur_job.job_msg['num_mappers']:
                        break
                    self.send_new_worker_job(worker, job_parts[i],
                                             job['output_directory'],
                                             job['executable']
                                             )
                else:
                    self.send_new_sort_job(
                        worker, job_parts[i],
                        job['output_file'][:-2] + worker.args['output_id']
                    )
                worker.status = 'BUSY'

    def send_new_worker_job(self, worker, input_files, out_dir, exe_file):
        """Add worker job to worker message queue."""
        new_job_msg = {
            "message_type": "new_worker_job",
            "input_files": input_files,
            "executable": exe_file,
            "output_directory": out_dir,
            "worker_pid": worker.args['worker_pid']
        }
        worker = self.cur_job.workers[worker.args['worker_pid']]
        worker.add_job_msg(new_job_msg)

    def send_new_sort_job(self, worker, input_files, out_file):
        """Distribute sort job to worker."""
        new_job_msg = {
            "message_type": "new_sort_job",
            "input_files": input_files,
            "output_file": out_file,
            "worker_pid": worker.args['worker_pid']
        }
        worker = self.cur_job.workers[worker.args['worker_pid']]
        worker.add_job_msg(new_job_msg)

    def num_living_workers(self):
        """Count number of living workers."""
        return len([w for w in self.worker_map.values() if w.status != 'DEAD'])


def partition_job(input_dir, input_files, num_workers):
    """Partition job for workers."""
    job_list = [[] for i in range(num_workers)]
    i = 0
    # logging.info("Jobs are in %s", input_dir)
    # input_files = os.listdir(input_dir)
    for input_file in input_files:
        job_list[i].append(os.path.join(input_dir, input_file))
        i = (i + 1) % num_workers
    # logging.info("%s", job_list)
    return job_list


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    """Master process."""
    Master(port)


if __name__ == '__main__':
    # logging.info('start master')
    main()
