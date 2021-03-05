"""Worker.

Define Worker class.
"""
import os
import sys
import subprocess
import logging
import json
import socket
import time
import threading
import click
from mapreduce.utils import tcp_send, udp_send
# import signal
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)


class Worker:
    """Worker process listen to Master."""

    def __init__(self, master_port, worker_port):
        """Init Worker."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        self.threads = []
        self.master_port = master_port
        self.worker_port = worker_port
        self.worker_pid = os.getpid()
        self.heart_freq = 2
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('localhost', self.worker_port))
        self.server.listen(10)
        print('init')
        self.send_start()
        self.signal = {"shutdown": False}
        self.worker_server_listen()

    def send_start(self):
        """Register itself to Master."""
        data = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": self.worker_port,
            "worker_pid": self.worker_pid
        }
        try:
            tcp_send(data, self.master_port)
        except OSError:
            return

    def send_heartbeat(self):
        """Send heartbeat to Master using UDP."""
        logging.info('sending heartbeat')
        while not self.signal["shutdown"]:
            pid = self.worker_pid
            port = self.master_port - 1
            heartbeat_msg = {
                "message_type": "heartbeat",
                "worker_pid": pid
            }
            udp_send(heartbeat_msg, port)
            time.sleep(2)

    def worker_server_listen(self):
        """Listen to messages from Master."""
        while not self.signal["shutdown"]:
            data = bytearray()
            client, _ = self.server.accept()
            try:
                while True:
                    chunk = client.recv(4096)
                    if not chunk:
                        client.close()
                        break
                    data.extend(chunk)
                    # logging.info(data)
            except OSError:
                return {}
            try:
                msg = json.loads(data.decode('utf-8'))
            except ValueError:
                continue
            logging.info("Worker received: %s", msg)
            if msg["message_type"] == "new_worker_job":
                self.handle_new_worker_msg(msg)
            elif msg["message_type"] == "new_sort_job":
                self.handle_new_sort_msg(msg)
            elif msg["message_type"] == "register_ack":
                logging.info('register_ack')
                thread_heart = threading.Thread(target=self.send_heartbeat,
                                                daemon=True)
                thread_heart.start()
            elif msg['message_type'] == "shutdown":
                logging.info('should stop')
                self.signal = {"shutdown": True}
                self.threads.clear()
                self.server.close()
                break
        sys.exit(0)

    def handle_new_worker_msg(self, msg):
        """Handle new_worker_job message."""
        out_files = []
        try:
            for in_file in msg["input_files"]:
                out_file = os.path.join(msg["output_directory"],
                                        os.path.basename(in_file))
                in_text = open(in_file, "r")
                out_text = open(out_file, "w")
                subprocess.run(msg["executable"], stdin=in_text,
                               stdout=out_text,  # text=True,
                               check=True)
                in_text.close()
                out_text.close()
                logging.info("Finish %s on %s",
                             msg["executable"], in_file)
                out_files.append(out_file)
        except subprocess.CalledProcessError as job_fail:
            logging.info("Job exit with %d", job_fail.returncode)
            return
        # finish job
        finish_msg = {
            "message_type": "status",
            "output_files": out_files,
            "status": "finished",
            "worker_pid": self.worker_pid
        }
        try:
            tcp_send(finish_msg, self.master_port)
        except OSError:
            pass

    def handle_new_sort_msg(self, msg):
        """Handle new_sort_job message."""
        # sort input files into a single file
        key_value_list = []
        for in_file in msg["input_files"]:
            with open(in_file) as in_text:
                lines = in_text.readlines()
                if lines and lines[0]:
                    key_value_list += lines
        key_value_list.sort()
        if not os.path.exists(msg["output_file"]):
            file = open(msg["output_file"], 'a')
            file.close()
        with open(msg["output_file"], "w") as out_text:
            out_text.write("".join(key_value_list))
        # finish sorting
        finish_msg = {
            "message_type": "status",
            "output_file": msg["output_file"],
            "status": "finished",
            "worker_pid": self.worker_pid
        }
        try:
            tcp_send(finish_msg, self.master_port)
        except OSError:
            pass


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Worker process."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
