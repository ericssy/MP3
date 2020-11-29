
import logging
import socket
import thread
import json
import time
import random




def find_my_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    print(s.getsockname()[0])
    print(INTRODUCER_IP)
    return s.getsockname()[0]

JOINED = 'JOINED'
ADDED = 'ADDED'
SUSPECTED = 'SUSPECTED'
TO_QUIT = 'TO_QUIT'
QUIT = 'QUIT'
NEW_GRAD = 'NEW_GRAD'
AFTER_QUIT = 'AFTER_QUIT'

HEARTBEAT = 'heartbeat'
TIMESTAMP = 'timestamp'
STATUS = 'status'

POTENTIAL_FAIL = 'POTENTIAL_FAIL'
FAIL = 'FAIL'
POTENTIAL_FAIL_INTERVAL = 2
FAIL_INTERVAL = 2

INTRODUCER_IP = '172.22.94.68'
PORT_NUMBER = 2345
PORT_NUMBER_FILE = 2378
PORT_NUMBER_MAPLEJUICE = 2333

TALK_REST_INTERVAL = 0.2

GOSSIP = 'GOSSIP'
ALLTOALL = 'ALLTOALL'
UNKNOWN = 'UNKNOWN'

NUMBER_OF_GOSSIP = 2
MSG_LOSS_RATE = 5


GET = 'get'
PUT = 'put'
DELETE = 'delete'
ACK = 'ack'
SUOLUETU = 'suoluetu'

OP = 'op'
REQUEST_IP = 'request_ip'
SDFS_FILE_NAME = 'sdfsfilename'
LOCAL_FILE_NAME = 'localfilename'
FILE_SENDER_IP = 'filesenderip'
FILE_TARGET_IP = 'filetargetip'
TO_DO = 'to_do'

WRITE = 'write'

DELETE_RESULT = 'delete_result'
FILE_AVAILABLE = 'FILE_AVAILABLE'

IP_LIST = []

DIRECTORY = "/home/jiaxusu2/"

### MapleJuice
HASHED_KEY = "hashed_key"
MAPLE_EXE = "maple_exe"
JUICE_EXE = "juice_exe"
SDFS_TMP_FILENAME_PREFIX = "sdfs_tmp_filename_prefix"
SOURCE_FILES = "source_files"
SDFS_DEST_FILENAME = "sdfs_dest_filename"
DELETE_INPUT = "delete_input"

MAPLE_TASK_COMPLETE = "maple task complete"
JUICE_TASK_COMPLETE = "juice task complete"
WORKER_VM_SENT_MAPLE_TO_MASTER = "Worker VM sent MAPLE to Master"
WORKER_VM_SENT_JUICE_TO_MASTER = "Worker VM sent JUICE to Master"
CMD_SPLIT = "cmd_split"
