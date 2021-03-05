"""Utils file.

This file is to house code common between the Master and the Worker

"""
import socket
import json
import logging

HEART_FREQ = 2


def tcp_send(data, port, host='localhost'):
    """Connect to TCP server and send data."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # sock.settimeout(HEART_FREQ * 5)
    try:
        logging.info("Sending %s", data)
        sock.connect((host, port))
        sock.sendall(json.dumps(data).encode('utf-8'))
        sock.close()
    except OSError:
        return


def udp_send(data, port, host='localhost'):
    """Send data using UDP."""
    print('udp_send')
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # sock.settimeout(HEART_FREQ * 5)
    try:
        logging.info("Sending UDP packet to %d: %s", port, data)
        sock.connect((host, port))
        sock.sendall(json.dumps(data).encode('utf-8'))
        sock.close()
    except OSError:
        return
