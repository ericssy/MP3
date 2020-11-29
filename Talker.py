
import logging
import uuid

import thread
import threading

import MemberList
import Util
from MemberList import MemberList
from Util import *
import random
# import scpHelper
from scpHelper import scpFileTransfer
import time
from ast import literal_eval

import glob
import sys 
import os



logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO,
                    filemode='w',
                    filename = 'logFile_' + str(int(time.time()))
                    )
LOGGER = logging.getLogger("main")

member_list_lock = thread.allocate_lock()
senderQueueLock = thread.allocate_lock()
receiverQueueLock = thread.allocate_lock()
waitingLock = thread.allocate_lock()
myCurrentOperationLock = thread.allocate_lock()
MapleJuice_trackerLock = thread.allocate_lock()

class Talker(object):

    def __init__(self, spread_type, isMaster):
        super(Talker, self).__init__()

        self.ip = Util.find_my_ip()
        temp_id = uuid.uuid1()
        self.id = str(self.ip) + '_' + str(temp_id.int)[:5]
        self.status = 'JOINED'
        self.timestamp = time.time()
        self.heartbeat = 1
        self.membershipList = MemberList(self)
        # why need tuple for spreadType, we need this later for changing spread type
        self.spread_type = (spread_type, time.time())

        # 0 index is master ip, 1 index is heartbeat
        self.master_info = [-1, 0]
        self.vm_file_queue = []
        self.vm_dict = []

        if isMaster:
            self.master_info[0] = self.ip
            self.master_info[1] += 1

        self.master_queue = []
        # self.sendQueue = []
        self.receiveQueue = []
        self.processingOrNot = False
        self.suoLueTu = {}
        self.MyCurrentOperation = ""
        self.waiting = False

        ## MapleJuice 
        self.mapleJuiceQueue = []
        self.MapleJuice_tracker = {}
        

    def isThisIpMaster(self):
        if self.master_info[0] == self.ip:
            return True
        return False

    def hasMaster(self):
        if self.master_info[0] != -1:
            return True
        return False

    def updateSuoleutuBecuaseOfFailure(self, failedVms):

        for fileName, listOfIds in self.suoLueTu.iteritems():

            for vm in failedVms:
                if vm in self.suoLueTu[fileName]:
                    self.suoLueTu[fileName].remove(vm)

    def updateMapleJuice_tracker(self, failedVms):
        print("Detect failure. Possible reassigned")
        for vm in failedVms:
            MapleJuice_trackerLock.acquire()
            if vm in self.MapleJuice_tracker:
                # select new vm: new_worker
                print("I need a new worker")
                new_worker = self.get_new_worker()
                print("I find a new worker")
                old_msg = self.MapleJuice_tracker[vm].copy()
                del self.MapleJuice_tracker[vm]
                old_msg["target_ip"] = new_worker
                self.MapleJuice_tracker[new_worker] = old_msg
                print("Update maple juice tracker ")
                self.send_message_through_socket_mapleJuice(old_msg, new_worker)
            MapleJuice_trackerLock.release()
        print("Finish update maple juice tracker ")

    def get_new_worker(self):
        for vm_id in self.membershipList.members:
            ip = vm_id.split("_")[0]
            if (ip not in self.MapleJuice_tracker and ip != self.master_info[0]):
                return ip
        print("no new worker available")

    def grouptalk(self):
        
        while True:
            # we need this lock to avoid deadlock 
            member_list_lock.acquire()

            if self.status == 'JOINED' or self.status == TO_QUIT:
                # refresh first in here to take care of the potential fail and fail before talking to others
                failedVms = self.membershipList.refresh()

                if len(failedVms) > 0:
                    self.updateSuoleutuBecuaseOfFailure(failedVms)
                    self.updateMapleJuice_tracker(failedVms)
                
                self.heartbeat_increment()

                if self.master_info[0] == -1:
                    pass

                message = self.membershipList.rumorGeneration()
                message = {'spread_type': self.spread_type, 'message': message, 'master_info': self.master_info}

                if self.isThisIpMaster():
                    message[SUOLUETU] = self.suoLueTu

                memberList = self.membershipList.talkableMembers()

                # generate listener list based on gossip or alltoall
                if self.spread_type[0] == GOSSIP:
                    memberList = random.sample(memberList, min(len(memberList), NUMBER_OF_GOSSIP))
                self.sendMessageToEveryone(message, memberList)

                if (self.membershipList.members[INTRODUCER_IP][STATUS] == TO_QUIT and self.ip != INTRODUCER_IP):
                    self.membershipList.members[INTRODUCER_IP] = {
                        HEARTBEAT: 0,
                        STATUS: NEW_GRAD,
                        TIMESTAMP: 0
                    }

                if self.status == 'JOINED':
                    # everytime I send out a message to others, I will clean my list if my memberlist is changing to TO_QUIT
                    self.membershipList.toQuitRefresh()

                elif self.status == TO_QUIT:
                    # if I change my status to TO_QUIT, then I will change it to afterquit in order to quit
                    self.status = AFTER_QUIT

            member_list_lock.release()
            # everytime I wait 0.2 seconds in order to send another messge
            time.sleep(float(TALK_REST_INTERVAL))

    def sendMessageToEveryone(self, message, memberList):
        logging.info('This is the MemberList: \n'+ str(memberList) + '\n')
        logging.info('This is the message I am going to send to above Memberlist  : \n'+ str(message)+ '\n')

        for memberIp in memberList:
            # I dont send message to myself
            if memberIp == self.id.split('_')[0]:
                continue
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                sock.sendto(json.dumps(message), (memberIp, PORT_NUMBER))

            except Exception:
                pass

    def heartbeat_increment(self):
        self.heartbeat += 1
        self.timestamp = time.time()


    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.ip, PORT_NUMBER))
        while self.status == 'JOINED':
            payload = json.loads(sock.recvfrom(2048)[0].strip())
            logging.info('This is the Rumor I receive: \n'+ str(payload)+ '\n')

            rumor = payload['message']
            spreadType = payload['spread_type']
            curr_master_info = payload["master_info"]

            if SUOLUETU in payload:
                #this is from muaster
                self.suoLueTu = payload[SUOLUETU]

            if curr_master_info[1] > self.master_info[1]:
                self.master_info = curr_master_info
                print("This VM's master is changed to: ", self.master_info)
            elif curr_master_info[1] == self.master_info[1] and curr_master_info[0] != self.master_info[0]:
                print("BUG !!!!!!!!!!!!!!!, LINE 138")
                print(curr_master_info, self.master_info)

            # this part is to decide if I need to change the spreadType
            prev = self.spread_type[0]
            if self.spread_type[0] == UNKNOWN and spreadType[0] != UNKNOWN:
                self.spread_type = spreadType
            elif spreadType[0] != UNKNOWN and spreadType[1] > self.spread_type[1]:
                self.spread_type = spreadType
            after = self.spread_type[0]
            if prev != after:
                logging.info("I am changing spread type to " + after)
                print("I am changing spread type to " + after)
            if rumor:
                member_list_lock.acquire()

                self.membershipList.merge(rumor)

                # check if master still running
                if not self.membershipList.checkIpExist(self.master_info[0]):

                    # update new master
                    # get largest list ip largest_ip
                    # TODO: may have inconsistency ???

                    largest_ip = self.membershipList.getLargestIp()
                    self.master_info[0] = largest_ip
                    self.master_info[1] += 1
                    print("This VM's master is changed to: ", self.master_info)
                else:
                    if self.master_info[0] == INTRODUCER_IP and self.membershipList.members[INTRODUCER_IP][STATUS] == NEW_GRAD:
                        largest_ip = self.membershipList.getLargestIp()
                        self.master_info[0] = largest_ip
                        self.master_info[1] += 1
                        print("This VM's master is changed to: ", self.master_info)

                member_list_lock.release()



    def fileOp(self, cmd):
        # print("file op initiated")
        # while True:
        #     while self.MyCurrentOperation != '':
        #         continue
        #     print("while loop ends")
            #cmd = raw_input('input file op: \n')
            # print("cmd ends")
        cmd_array = cmd.split(' ')


        # senderQueueLock.acquire()
        if cmd_array[0] == 'get':
            if len(cmd_array) != 3:
                print("wrong input for get")
                return
            sdfsfilename = cmd_array[1]
            localfilename = cmd_array[2]
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = {"op": cmd_array[0], "sdfsfilename": sdfsfilename,
                                       "localfilename": localfilename, "request_ip" : self.ip}
            myCurrentOperationLock.release()
            message = json.dumps(self.MyCurrentOperation)
            while not self.hasMaster():
                continue

            if (not self.isThisIpMaster()):
                self.send_message_through_socket(message, self.master_info[0])
            else:
                waitingLock.acquire()
                self.master_queue.append(self.MyCurrentOperation)
                waitingLock.release()
        
        elif cmd_array[0] == 'put':
            if len(cmd_array) != 3:
                print("wrong input for put")
                return
            localfilename = cmd_array[1]
            sdfsfilename = cmd_array[2]
            # send put request to master
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = {"op": cmd_array[0], "sdfsfilename": sdfsfilename,
                                       "localfilename": localfilename, "request_ip": self.ip}
            myCurrentOperationLock.release()
            message = json.dumps(self.MyCurrentOperation)
            while not self.hasMaster():
                continue

            if (not self.isThisIpMaster()):
                self.send_message_through_socket(message, self.master_info[0])
            else:
                waitingLock.acquire()
                self.master_queue.append(self.MyCurrentOperation)
                waitingLock.release()

        elif cmd_array[0] == 'delete':
            if len(cmd_array) != 2:
                print("wrong input for delete")
                return
            sdfsfilename = cmd_array[1]
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = {"op": "delete", "sdfsfilename": sdfsfilename, REQUEST_IP : self.ip}
            myCurrentOperationLock.release()
            message = json.dumps(self.MyCurrentOperation)
            while not self.hasMaster():
                continue

            if (not self.isThisIpMaster()):
                self.send_message_through_socket(message, self.master_info[0])
            else:
                # if the vm is master, no need to send it through socket, just append it to the queue
                waitingLock.acquire()
                self.master_queue.append(self.MyCurrentOperation)
                waitingLock.release()

            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = ""
            myCurrentOperationLock.release()

        elif cmd_array[0] == 'ls':
            if len(cmd_array) != 2:
                print("wrong input for ls")
                return

            sdfsfilename = cmd_array[1]

            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = {"op": "ls", "sdfsfilename": sdfsfilename}

            if (sdfsfilename in self.suoLueTu):
                print("The SDFS file ", sdfsfilename, " is located in the following VMs: ")
                for vm_ip in self.suoLueTu[sdfsfilename]:
                    print(vm_ip)
            else:
                print("The SDFS file ", sdfsfilename, " is not available")

            self.MyCurrentOperation = ""
            myCurrentOperationLock.release()

        elif cmd_array[0] == 'store':
            print("store invoked", cmd_array)
            if len(cmd_array) != 1:
                print("wrong input for store")
                return
            # sdfs_return_list = []

            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = {"op": "store"}
            print("All files stored at this machine: ")
            for sdfsfile in self.suoLueTu:
                if (self.ip in self.suoLueTu[sdfsfile]):
                    # sdfs_return_list.append(sdfsfile)
                    print(sdfsfile)

            self.MyCurrentOperation = ""
            myCurrentOperationLock.release()

        else:
            print("Input error. Retry!")

        # senderQueueLock.release()

    def fileOp_master(self):
        while True:
            if (not self.isThisIpMaster()):
                continue

            waitingLock.acquire()
            if (len(self.master_queue) == 0):
                waitingLock.release()
                continue

            if (self.waiting == True):
                waitingLock.release()
                continue

            # waiting = False
            task = self.master_queue[0].copy()
            waitingLock.release()


            # print("fileop_master Queue: ", task)
            op = task["op"]
            if op == GET:
                # send this tast["request_ip"] it can do!!
                sdfsFileName = task['sdfsfilename']
                # TODO we might need to handle replica down in here
                if task[REQUEST_IP] != self.ip:
                    # print("GET: sdfsFile ", sdfsFileName, " suoleutu: ", self.suoLueTu)
                    if sdfsFileName in self.suoLueTu:
                        ret = self.suoLueTu[sdfsFileName][0]
                        task[FILE_SENDER_IP] = ret
                        task[TO_DO] = WRITE
                        task[FILE_AVAILABLE] = True
                        self.send_message_through_socket(task,task[FILE_SENDER_IP])
                        waitingLock.acquire()
                        self.waiting = True
                        waitingLock.release()
                    else:
                        task[FILE_AVAILABLE]  = False
                        task[ACK] = True
                        self.send_message_through_socket(task,task[REQUEST_IP])
                        waitingLock.acquire()
                        self.waiting = False
                        self.master_queue.pop(0)
                        waitingLock.release()
                else:
                    # master make this request
                    if sdfsFileName in self.suoLueTu:
                        ret = self.suoLueTu[sdfsFileName][0]
                        task[FILE_SENDER_IP] = ret
                        task[TO_DO] = WRITE
                        task[FILE_AVAILABLE] = True
                        self.send_message_through_socket(task, task[FILE_SENDER_IP])
                        waitingLock.acquire()
                        self.waiting = True
                        waitingLock.release()
                    else:
                        print("File ", sdfsFileName, " not available")

                        myCurrentOperationLock.acquire()
                        self.MyCurrentOperation = ''
                        myCurrentOperationLock.release()

                        waitingLock.acquire()
                        self.waiting = False
                        self.master_queue.pop(0)
                        waitingLock.release()


            if op == PUT:
                # we need to update the suoluetu
                # if it is a new put
                sdfsFileName = task[SDFS_FILE_NAME]
                
                # if suoluotu contains sdfsFileName
                if sdfsFileName not in self.suoLueTu:
                    # we need to find 4 machines to the requester so that he can send files to them
                    requesterIp = task[REQUEST_IP]
                    # print("this is request ip for create new file:" + requesterIp + "\n this is my ip: " + self.ip)
                    # print("get4VmActiveAddress request ip: " + requesterIp)
                    listofFourVms = self.get4VmActiveAddress(requesterIp)

                    # self.suoLueTu[sdfsFileName] = listofFourVms
                    # print("PUT message sent from Master")
                    task[TO_DO] = WRITE
                    task[FILE_TARGET_IP] = listofFourVms

                    # if (self.isThisIpMaster() == False):
                    if requesterIp != self.ip:

                        self.send_message_through_socket(task, requesterIp)
                        waitingLock.acquire()
                        self.waiting = True
                        waitingLock.release()
                    else:
                        # this put request is made by master
                        sdfsfilename = task['sdfsfilename']
                        localfilename = task['localfilename']
                        threads = []

                        for targetIP in task[FILE_TARGET_IP]:
                            toHostName = targetIP
                            fromFile = DIRECTORY + localfilename
                            toFile = DIRECTORY + "sdfs_dir/" + sdfsfilename
                            # print("invoke scpFileTransfer" + " " + toHostName + " " + fromFile + " " + toFile)
                            #scpFileTransfer(toHostName, fromFile, toFile)
                            t = threading.Thread(target=scpFileTransfer, args=(toHostName, fromFile, toFile, ))
                            threads.append(t)
                            t.start()
                        
                        for t in threads:
                            t.join()

                        print(" scpFileTransfer complete")

                        self.suoLueTu[sdfsFileName] = listofFourVms

                        waitingLock.acquire()
                        self.waiting = False
                        self.master_queue.pop(0)
                        waitingLock.release()

                        myCurrentOperationLock.acquire()
                        self.MyCurrentOperation = ""
                        myCurrentOperationLock.release()

                        print("PUT " + localfilename + " to " + sdfsfilename + " completed")

                else:
                    requesterIp = task[REQUEST_IP]
                    ### requesterIp == self.ip == Master IP
                    ## check if request IP equals to master ip 
                    if (requesterIp == self.ip):
                        task[TO_DO] = WRITE
                        task[FILE_TARGET_IP] = self.suoLueTu[sdfsFileName]
                        threads = []

                        for targetIP in task[FILE_TARGET_IP]:
                            toHostName = targetIP
                            fromFile = DIRECTORY + localfilename
                            toFile = DIRECTORY + "sdfs_dir/" + sdfsfilename
                            # print("invoke scpFileTransfer" + " " + toHostName + " " + fromFile + " " + toFile)
                            #scpFileTransfer(toHostName, fromFile, toFile)
                            t = threading.Thread(target=scpFileTransfer, args=(toHostName, fromFile, toFile, ))
                            threads.append(t)
                            t.start()
                        for t in threads:
                            t.join()
                        print(" scpFileTransfer complete" + " " + toHostName + " " + fromFile + " " + toFile)

                        waitingLock.acquire()
                        self.waiting = False
                        self.master_queue.pop(0)
                        waitingLock.release()

                        myCurrentOperationLock.acquire()
                        self.MyCurrentOperation = ""
                        myCurrentOperationLock.release()
                        print("PUT " + localfilename + " to " + sdfsfilename + " completed")
     
                    else:
                        requesterIp = task[REQUEST_IP]
                        task[TO_DO] = WRITE
                        task[FILE_TARGET_IP] = self.suoLueTu[sdfsFileName]
                        self.send_message_through_socket(task, requesterIp)
                        waitingLock.acquire()
                        self.waiting = True
                        waitingLock.release()

                    # this is gonna be an update, I will not update suoluetu
            if op == DELETE:
                # TODO!!!!!! Tell requesting VM whether the file had been deleted successfully
                waitingLock.acquire()
                self.waiting = True
                waitingLock.release()
                sdfsFileName = task[SDFS_FILE_NAME]
                if (sdfsFileName in self.suoLueTu):
                    del self.suoLueTu[sdfsFileName]
                    task["result"] = "Delete: success"
                else:
                    task["result"] = "Delete: file not exist"

                ## add fields to task
                task[ACK] = True
                if not self.isThisIpMaster():
                    self.send_message_through_socket(task, task[REQUEST_IP])
                else:
                    print(task["result"])

                waitingLock.acquire()
                self.master_queue.pop(0)
                self.waiting = False
                waitingLock.release()



    def get4VmActiveAddress(self, requesterIp):
        # TODO maybe id is wrong

        preRet = []

        for ip in self.membershipList.talkableMembers():
            ip = ip.split('_')[0]
            if ip == requesterIp:
                continue
            preRet.append(ip)
        if requesterIp != self.ip:
            # I must be the master, since I am deciding 4 replica
            preRet.append(self.ip)
        print("preRet: ", preRet)
        return random.sample(preRet, min(4, len(preRet)))

    def fileOp_listen(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.ip, PORT_NUMBER_FILE))

        while True:
            if self.isThisIpMaster() == False:
                # I am not master!!
                payload = json.loads(s.recvfrom(2048)[0].strip())
                print("fileOp_listen Non master payload received: ", payload)
                try:
                    payload = literal_eval(payload)
                except:
                    payload = payload

                print("from fileop_listen : ", payload)
                # payload = literal_eval(payload)
                # message = {"op": "put", "sdfsfilename": sdfsfilename, "localfilename": localfilename}
                op = payload['op']
                # this part deal with put and get from small vms
                task = payload.copy()
                if op == GET:
                    sdfsfilename = payload['sdfsfilename']
                    localfilename = payload['localfilename']
                    if ACK in payload:
                        # if this is an ack remove it from self.MyCurrentOperation
                        # A receive ack
                        if (FILE_AVAILABLE in task and task[FILE_AVAILABLE]):
                            print("get completed, I received ack, filename is ", localfilename)
                        else:
                            print("File ", sdfsfilename, " not available")
                        myCurrentOperationLock.acquire()
                        self.MyCurrentOperation = ""
                        myCurrentOperationLock.release()


                    if TO_DO in payload and payload[TO_DO] == WRITE:
                        # when B receive this
                        # master wants me to write sdfsfilename to ip:localfilename
                        # when i am done, send ack to master & request_ip
                        # callscp, then give ack to master and A

                        # TODO might need to change MyCurrentOperation and add a lock
                        toHostName= payload[REQUEST_IP]
                        fromFile = DIRECTORY + "sdfs_dir/" + sdfsfilename
                        toFile = DIRECTORY + localfilename

                        scpFileTransfer(toHostName, fromFile, toFile)

                        task[ACK] = True
                        task[TO_DO] = None
                        task[FILE_AVAILABLE] = True
                        if task[REQUEST_IP] !=  self.master_info[0]:
                            self.send_message_through_socket(task, self.master_info[0])
                        self.send_message_through_socket(task, task[REQUEST_IP])

                elif op == PUT:

                    # this must be from master
                    sdfsfilename = payload['sdfsfilename']
                    localfilename = payload['localfilename']
                    print("Put Test", task)

                    # if ACK in payload:
                    #     # if this is an ack remove it from self.MyCurrentOperation
                    #     # A receive ack
                    #     print("put completed, I received ack. The file uploaded to SDFS is ", sdfsfilename)
                    #     # myCurrentOperationLock.acquire()
                    #     # self.MyCurrentOperation = ""
                    #     # myCurrentOperationLock.release()

                    if (TO_DO in task and task[TO_DO] == WRITE):
                        threads = []
                        for targetIP in task[FILE_TARGET_IP]:
                            toHostName = targetIP
                            fromFile = DIRECTORY + localfilename
                            toFile = DIRECTORY + "sdfs_dir/" + sdfsfilename
                            #print("invoke scpFileTransfer" + " " + toHostName + " " + fromFile + " " + toFile)
                            #scpFileTransfer(toHostName, fromFile, toFile)
                            t = threading.Thread(target=scpFileTransfer, args=(toHostName, fromFile, toFile, ))
                            threads.append(t)
                            t.start()
                        for t in threads:
                            t.join()
                        print(" scpFileTransfer complete" + " " + toHostName + " " + fromFile + " " + toFile)

                        # send ack to mster
                        task[ACK] = True
                        task[TO_DO] = None
                        self.send_message_through_socket(task, self.master_info[0])
                        print("PUT " + localfilename + " to " + sdfsfilename + " completed")

                        myCurrentOperationLock.acquire()
                        self.MyCurrentOperation = ""
                        myCurrentOperationLock.release()
                        print("LOCK TEST: myCurrentOp released ", self.MyCurrentOperation)
                elif op == DELETE:
                    if ACK in payload:
                        # if this is an ack remove it from self.MyCurrentOperation
                        # A receive ack
                        print(payload["result"])
                        myCurrentOperationLock.acquire()
                        self.MyCurrentOperation = ""
                        myCurrentOperationLock.release()


            else:
                # I am master !!
                payload = json.loads(s.recvfrom(2048)[0].strip())
                print(payload)
                try:
                    payload = literal_eval(payload)
                except:
                    payload = payload

                op = payload["op"]
                print("from fileop_listen : ", payload)

                if op == GET:
                    sdfsfilename = payload['sdfsfilename']
                    localfilename = payload['localfilename']

                    if 'ack' in payload:
                        # remove it from master_queue, pop it
                        waitingLock.acquire()
                        self.master_queue.pop(0)
                        self.waiting = False
                        waitingLock.release()
                        if self.ip == payload[REQUEST_IP]:
                            myCurrentOperationLock.acquire()
                            self.MyCurrentOperation = ''
                            myCurrentOperationLock.release()
                        print("GET completed")
                    else:
                        # get in the queue
                        message = {"op": payload['op'], "sdfsfilename": payload['sdfsfilename'], "localfilename": payload['localfilename'], "request_ip" : payload['request_ip']}
                        waitingLock.acquire()
                        self.master_queue.append(message)
                        waitingLock.release()
                elif op == PUT:
                    if 'ack' in payload:
                        # remove it from master_queue, pop it
                        sdfsfilename = payload['sdfsfilename']
                        self.suoLueTu[sdfsfilename] = payload[FILE_TARGET_IP]
                        print("current suoleutu updated", self.suoLueTu)
                        waitingLock.acquire()
                        self.master_queue.pop(0)
                        print("PUT completed")
                        self.waiting = False
                        waitingLock.release()
                    else:
                        # put in the queue
                        message = {"op": payload['op'], "sdfsfilename": payload['sdfsfilename'],
                                   "localfilename": payload['localfilename'], "request_ip" : payload['request_ip']}
                        waitingLock.acquire()
                        self.master_queue.append(message)
                        waitingLock.release()
                        print("fileop_listen PUT message add to master queue", message)


                elif op == DELETE:
                    # delete this sdfs from master_queue
                    waitingLock.acquire()
                    self.master_queue.append(payload)
                    waitingLock.release()
            s.close


    def send_message_through_socket(self, message, target_ip):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print("socket send message to  : ", str(target_ip), message)
        # sock.connect((target_ip, PORT_NUMBER_FILE))
        # sock.send(json.dumps(message))
        sock.sendto(json.dumps(message), (target_ip, PORT_NUMBER_FILE))
        sock.close
    
    def send_message_through_socket_mapleJuice(self, message, target_ip):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print("MapleJuice socket send message to  : ", str(target_ip), message)
        sock.sendto(json.dumps(message), (target_ip, PORT_NUMBER_MAPLEJUICE))
        sock.close

    def find_src_files_with_prefix(self, maple_src_files, sdfs_src_dir):
        for key in self.suoLueTu:
            if (sdfs_src_dir in key):
                maple_src_files.append(key)
        return maple_src_files

    # def mapleJuiceScheduler(self, cmd_split):
    #     task = cmd_split
    #     self.mapleJuiceQueue.append(task)
    #     while (len(self.mapleJuiceQueue) > )
    #     if (cmd_type == "maple"):
    #         maple_exe = cmd_split[1]
    #         num_maples = int(cmd_split[2])
    #         sdfs_tmp_filename_prefix = cmd_split[3]
    #         sdfs_src_dir = cmd_split[4]
            
    #     elif (cmd_type == "juice"):
    #         juice_exe = cmd_split[1]
    #         num_juices = int(cmd_split[2])





    # Maple Juice Implementation
    def start_maple_phase(self, cmd_split):
        cmd_type = cmd_split[0]
        maple_exe = cmd_split[1]
        num_maples = int(cmd_split[2])
        sdfs_tmp_filename_prefix = cmd_split[3]
        sdfs_src_dir = cmd_split[4]
        
        while (len(self.mapleJuiceQueue) > 0):
            continue
        
        
        self.mapleJuiceQueue.append(cmd_split)
        if self.isThisIpMaster():
            print("Maple Phase starts")
            
            ### get all the source files in sdfs_src_dir directory
            # TODO:
            maple_src_files = []
            # maple_src_files = glob.glob(DIRECTORY + "sdfs_dir/" + sdfs_src_dir + "*")
            maple_src_files = self.find_src_files_with_prefix(maple_src_files, sdfs_src_dir)
            print(maple_src_files)

            talkable_members = self.membershipList.talkableMembers()
            # if there's not enough machines 
            if (talkable_members < num_maples - 2):
                num_maples = talkable_members + 1
            
            # Partitioning with Hash 
            hash_table = {}
            for src_file in maple_src_files:
                hashed_key = int(sys.maxint) & abs(hash(src_file)) % num_maples
                print(hashed_key)
                if (hashed_key in hash_table):
                    hash_table[hashed_key].append(src_file)
                else:
                    hash_table[hashed_key] = []
                    hash_table[hashed_key].append(src_file)
            
            print("hash_table: ", hash_table)
            # vm_hashedKey_lookup = []
            # vm_hashedKey_lookup[self.ip] = 0
            # for i in range(num_maples):
            #     vm_hashedKey_lookup.append(talkable_members[i])

            #DONE: 
            # change num_maples to min(number of files, num_maples)
            num_maples = min(len(maple_src_files), num_maples)

            # self.MapleJuice_tracker = hash_table.copy()
            self.MapleJuice_tracker = {}
            ### Generate message and send it over to worker VMs. Keep the
            ### last one for Master itself. 
            for i in range(num_maples):
                if (i != num_maples - 1):
                    target_ip = talkable_members[i].split("_")[0]
                msg = {}
                msg[OP] = "Maple_start"
                msg[HASHED_KEY] = i
                msg[MAPLE_EXE] = maple_exe
                msg[SDFS_TMP_FILENAME_PREFIX] = sdfs_tmp_filename_prefix
                msg[SOURCE_FILES] = hash_table[i]
                msg["target_ip"] = target_ip 
                if (i != num_maples - 1):
                    self.send_message_through_socket_mapleJuice(msg, target_ip)
                    self.MapleJuice_tracker[target_ip] = msg
                

            #DONE: 
            # map_task() for master itself 
            self.maple_task(msg)

            print("--------[Maple !] The initial tasks has been assigned.----- ")

            #
            while (len(self.MapleJuice_tracker) > 0):
                continue 
            print("[Maple] Maple tasks from all Worker VM completed")
            ## all Maple task finsihed 
            ## Combine all output files from task 
            self.maple_phase_generate_outputs(hash_table, sdfs_tmp_filename_prefix)
            self.mapleJuiceQueue.pop(0)

        else:
            # DONE:
            ## send command to master 
            msg = {}
            msg[OP] = WORKER_VM_SENT_MAPLE_TO_MASTER
            msg[CMD_SPLIT] = cmd_split
            self.send_message_through_socket_mapleJuice(msg, self.master_info[0])
            self.mapleJuiceQueue.pop(0)
            return 
        

            


    def mapleJuice_listen(self): 
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.ip, PORT_NUMBER_MAPLEJUICE))
        print("MapleJuice Listen Start")
        while True:
            payload = json.loads(s.recvfrom(2048)[0].strip())
            print("MapleJuice listen received payload")
            try:
                payload = literal_eval(payload)
            except:
                payload = payload
            print("MapleJuice listener node payload: ", payload)
            
            if (self.isThisIpMaster() == False):
                if (payload[OP] == "Maple_start"):
                    self.maple_task(payload)
                    #TODO: need to update the num of tasks completed
                
                if (payload[OP] == "Juice_start"):
                    self.juice_task(payload)

            else:
            # Master 
                #DONE: 
                # need to update the num of tasks completed
                if (payload[OP] == MAPLE_TASK_COMPLETE):
                    MapleJuice_trackerLock.acquire()
                    del self.MapleJuice_tracker[payload["target_ip"]] 
                    print("----------[MAPLE !]vm ip: " + payload["target_ip"] + "is finished-----")
                    MapleJuice_trackerLock.release()
                
                if (payload[OP] == JUICE_TASK_COMPLETE):
                    MapleJuice_trackerLock.acquire()
                    del self.MapleJuice_tracker[payload["target_ip"]] 
                    MapleJuice_trackerLock.release()

                #DONE: receive command from Worker VM, create new thread
                if (payload[OP] == WORKER_VM_SENT_MAPLE_TO_MASTER):
                    thread.start_new_thread(self.start_maple_phase, (payload[CMD_SPLIT],))
                
                #TODO:
                # receive JUICE task from Worker VM, create new thread
                if (payload[OP] == WORKER_VM_SENT_JUICE_TO_MASTER):
                    thread.start_new_thread(self.start_juice_phase, (payload[CMD_SPLIT],))
                

            


    def maple_task(self, payload):
        maple_exe = payload[MAPLE_EXE]
        sdfs_tmp_filename_prefix = payload[SDFS_TMP_FILENAME_PREFIX]
        source_files_needed = payload[SOURCE_FILES]
        hashed_key = payload[HASHED_KEY]

        # sdfs  Get
        for i in range(len(source_files_needed)):
            cmd_get = "get" + " " + source_files_needed[i] + " " + source_files_needed[i]
            # change MyCurrentOperation
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = "*"
            myCurrentOperationLock.release()
            thread.start_new_thread(self.fileOp, (cmd_get,))
            while self.MyCurrentOperation != '':
                continue
            print("[Maple] Get sdfs file " + source_files_needed[i] + " finished ")
        
        # invoke the maple_exe to read the input file 
        output_filename = sdfs_tmp_filename_prefix + "_" + str(hashed_key)
        output_file = open(DIRECTORY + output_filename, "w")
        
        # cmd_upper_dir = "cd .."
        # os.system(cmd_upper_dir)
        # invoke the maple_exe to read the input file
        # os.system blocks your python code until the bash command has finished thus you'll know that it is finished when os.system return
        for i in range(len(source_files_needed)):
            # python word_cnt.py input.txt >> output_file
            cmd_invoke_maple_exe = "python " + maple_exe + " " + DIRECTORY + source_files_needed[i] + " >> " + DIRECTORY + output_filename
            os.system(cmd_invoke_maple_exe)
            print("Maple exe finished")
        output_file.close()

        if (not self.isThisIpMaster()):
            # send file through scp
            scpFileTransfer(self.master_info[0], DIRECTORY + output_filename, DIRECTORY + output_filename)
            # TODO: delete intermediate file after scp transfer 
            # send message back to Master 
            payload[OP] = MAPLE_TASK_COMPLETE
            self.send_message_through_socket_mapleJuice(payload, self.master_info[0])
        else:
            # Master 
            # MapleJuice_trackerLock.acquire()
            # del self.MapleJuice_tracker[payload[HASHED_KEY]]
            # MapleJuice_trackerLock.release()
            return 

    def maple_phase_generate_outputs(self, hash_table, sdfs_tmp_filename_prefix):
        '''
        hashed_key :  list of pairs 
        0  :  [(hello, 1), (World, 1), (hello, 1)]
        

        word 
        '''
        dict_ = {}
        files = []
        for file_id in hash_table:
            filename = DIRECTORY + sdfs_tmp_filename_prefix + "_" + str(file_id)
            f = open(filename, 'r')
            lines = f.readlines()
            f.close()
            for line in lines:
                line = eval(line)
                key = line[0]
                value = line[1]
                # hashed_key = int(sys.maxint) & abs(hash(key)) % 10
                if (key in dict_):
                    dict_[key].append(str(value))
                else:
                    dict_[key] = [str(value)]
        
        vm_to_keys_map = {}
        for key in dict_:
            hashed_key = int(sys.maxint) & abs(hash(key)) % 10
            if (hashed_key in vm_to_keys_map):
                vm_to_keys_map[hashed_key].append((key, dict_[key]))
            else:
                vm_to_keys_map[hashed_key] = []
                vm_to_keys_map[hashed_key].append((key, dict_[key]))


        # TODO: Delete intermediate files sent from Worker vm 
        self.delete_file(sdfs_tmp_filename_prefix)

        for hashed_key in vm_to_keys_map:
            filename = sdfs_tmp_filename_prefix + "_" + "output" + "_" + str(hashed_key) 
            f = open(DIRECTORY + filename, 'w')
            for pair in vm_to_keys_map[hashed_key]:
                f.write(str(pair))
                f.write("\n")
            f.close()
            files.append(filename)
        

        ## upload the files to SDFS 
        for file in files:
            cmd_put = "put" + " " + file + " " + file
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = "*"
            myCurrentOperationLock.release()
            thread.start_new_thread(self.fileOp, (cmd_put,))
            while self.MyCurrentOperation != '':
                continue
        print("[Maple] all files uploaded to SDFS")
        print("[Maple] Maple phase complete")


    def put_with_prefix(self, cmd_split):
        prefix = cmd_split[1]
        files = glob.glob(DIRECTORY + prefix + "*")
        for file in files:
            file = file.split("/")[-1]
            cmd_put = "put" + " " + file + " " + file
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = "*"
            myCurrentOperationLock.release()
            thread.start_new_thread(self.fileOp, (cmd_put,))
            while self.MyCurrentOperation != '':
                continue
        print("Put Prefix Complete")
        
    def start_juice_phase(self, cmd_split):
        juice_exe = cmd_split[1]
        num_juice = int(cmd_split[2])
        sdfs_tmp_filename_prefix = cmd_split[3]
        sdfs_dest_filename = cmd_split[4]
        if (cmd_split[5].strip() == '0'):
            delete_input = False
        else:
            delete_input = True
        
        while (len(self.mapleJuiceQueue) > 0):
            continue
        
        self.mapleJuiceQueue.append(cmd_split)

        if self.isThisIpMaster():
            print("Juice Phase starts")
            juice_src_files = []
            juice_src_files = self.find_src_files_with_prefix(juice_src_files, sdfs_tmp_filename_prefix)
            print("Juice Source File: ", juice_src_files)

            talkable_members = self.membershipList.talkableMembers()

            if (talkable_members < num_juice - 2):
                num_juice = talkable_members + 1

            # Partitioning with Hash 
            hash_table = {}
            for src_file in juice_src_files:
                hashed_key = int(sys.maxint) & abs(hash(src_file)) % num_juice
                print(hashed_key)
                if (hashed_key in hash_table):
                    hash_table[hashed_key].append(src_file)
                else:
                    hash_table[hashed_key] = []
                    hash_table[hashed_key].append(src_file)
            
            print("hash_table: ", hash_table)
            num_juice = min(len(juice_src_files), num_juice)

            # self.MapleJuice_tracker = hash_table.copy()
            self.MapleJuice_tracker = {}
            cnt = 0
            for key in hash_table:
                cnt += 1
                if (cnt != len(hash_table)):
                    target_ip = talkable_members[key].split("_")[0]
                msg = {}
                msg[OP] = "Juice_start"
                msg[HASHED_KEY] = key
                msg[JUICE_EXE] = juice_exe
                msg[SDFS_DEST_FILENAME] = sdfs_dest_filename
                msg[SOURCE_FILES] = hash_table[key] 
                msg[DELETE_INPUT] = delete_input
                msg["target_ip"] = target_ip
                if (cnt != len(hash_table)):
                    self.send_message_through_socket_mapleJuice(msg, target_ip)
                    self.MapleJuice_tracker[target_ip] = msg

            # juice_task for master itself
            self.juice_task(msg)

            # if detecting a machine fail, reallocate the missing task
            print("[----Juice----] Msg sent to all worker VMs")

            while (len(self.MapleJuice_tracker) > 0):
                continue 
            
            print("[Juice] Juice tasks from all Worker VM completed")
            ## all Juice task finsihed 
            ## TODO: 
            ## Combine all output files from task 
            self.juice_phase_generate_output(hash_table, sdfs_dest_filename)
            # DONE: Delete intermediate file 
            if (delete_input == True):
                # sdfs_tmp_filename_prefix
                self.delete_file(sdfs_tmp_filename_prefix)

            self.mapleJuiceQueue.pop(0)
        
        else:
            # DONE:
            ## send command to master 
            msg = {}
            msg[OP] = WORKER_VM_SENT_JUICE_TO_MASTER
            msg[CMD_SPLIT] = cmd_split
            self.send_message_through_socket_mapleJuice(msg, self.master_info[0])
            self.mapleJuiceQueue.pop(0)
            return 

    def delete_file(self, filename_prefix):
        cmd_delete_intermediate = "rm -r " + DIRECTORY + filename_prefix + "*"
        os.system(cmd_delete_intermediate)
        print(cmd_delete_intermediate)
        print("Juice delete intermediate file")
        

    def juice_task(self, payload):
        juice_exe = payload[JUICE_EXE]
        source_files_needed = payload[SOURCE_FILES]
        sdfs_dest_filename = payload[SDFS_DEST_FILENAME]
        hashed_key = payload[HASHED_KEY]
        delete_input = payload[DELETE_INPUT]

        # sdfs get 
        for i in range(len(source_files_needed)):
            cmd_get = "get" + " " + source_files_needed[i] + " " + source_files_needed[i]
            # change MyCurrentOperation
            myCurrentOperationLock.acquire()
            self.MyCurrentOperation = "*"
            myCurrentOperationLock.release()
            thread.start_new_thread(self.fileOp, (cmd_get,))
            while self.MyCurrentOperation != '':
                continue
            print("[Juice] Get sdfs file " + source_files_needed[i] + " finished ")

        # invoke the maple_exe to read the input file 
        output_filename = sdfs_dest_filename + "_" + str(hashed_key)
        output_file = open(DIRECTORY + output_filename, "w")

        for i in range(len(source_files_needed)):
            # python word_cnt.py input.txt >> output_file
            cmd_invoke_juice_exe = "python " + juice_exe + " " + DIRECTORY + source_files_needed[i] + " >> " + DIRECTORY + output_filename
            os.system(cmd_invoke_juice_exe)
            print("Juice exe finished")
        output_file.close()

        if (not self.isThisIpMaster()):
            # send file through scp
            scpFileTransfer(self.master_info[0], DIRECTORY + output_filename, DIRECTORY + output_filename)
            # TODO: delete intermediate file after scp transfer 
            # send message back to Master 
            payload[OP] = JUICE_TASK_COMPLETE
            self.send_message_through_socket_mapleJuice(payload, self.master_info[0])
        else:
            # Master 
            # MapleJuice_trackerLock.acquire()
            # del self.MapleJuice_tracker[payload[HASHED_KEY]]
            # MapleJuice_trackerLock.release()
            print("Master juice task finished")
            return 

    def juice_phase_generate_output(self, hash_table, sdfs_dest_filename):
        output_file = DIRECTORY + sdfs_dest_filename
        file = open(output_file, "w")
        for file_id in hash_table:
            cmd = "cat " + DIRECTORY + sdfs_dest_filename + "_" + str(file_id) + " >> " + output_file
            print("execute cmd: ", cmd)
            os.system(cmd)
        file.close()
        
        # put file to sdfs 
        cmd_put = "put" + " " + sdfs_dest_filename + " " + sdfs_dest_filename
        myCurrentOperationLock.acquire()
        self.MyCurrentOperation = "*"
        myCurrentOperationLock.release()
        thread.start_new_thread(self.fileOp, (cmd_put,))
        while self.MyCurrentOperation != '':
            continue
        print("[Juice] Juice phase complete")


    def run(self):
        thread.start_new_thread(self.grouptalk, ())
        thread.start_new_thread(self.listen, ())
        #thread.start_new_thread(self.fileOp, ())
        thread.start_new_thread(self.fileOp_listen, ())
        thread.start_new_thread(self.fileOp_master, ())
        ## New thread to receive MapleJuice Tasks
        thread.start_new_thread(self.mapleJuice_listen, ())


        # grouptalk_th.start()
        # fileOp_th.start()

        while True:
            cmd = raw_input(' type "list" to show membership list\n type "my_id" to show the id of the local vm \n type "leave" to let the program voluntarily leave\n')
            cmd_split = cmd.split(" ")
            if cmd in ['gossip', 'alltoall']:
                member_list_lock.acquire()
                if cmd == 'gossip':
                    self.spread_type = (GOSSIP, time.time())
                else:
                    self.spread_type = (ALLTOALL, time.time())
                member_list_lock.release()
            elif cmd == 'list':
                print(self.membershipList)

            elif cmd == 'leave':
                member_list_lock.acquire()

                self.status = TO_QUIT
                member_list_lock.release()

                logging.info('I am leaving ')

                while self.status != AFTER_QUIT:
                    pass
                logging.info('I left ')
                break

            elif cmd == 'my_id':
                print(self.id)

            elif cmd == 'master':
                print(self.master_info)
            
            elif (cmd_split[0] == "maple"):
                if (len(cmd_split) < 5):
                    print("Maple command incorret")
                self.start_maple_phase(cmd_split)

            elif (cmd_split[0] == "juice"):
                if (len(cmd_split) < 6):
                    print("Juice command incorret")
                self.start_juice_phase(cmd_split)

            elif (cmd_split[0] == "put_prefix"):
                self.put_with_prefix(cmd_split)

            elif (len(cmd_split) >= 2 or cmd_split[0] == "store"):
                # self.fileOp(cmd)
                thread.start_new_thread(self.fileOp, (cmd,))
                while self.MyCurrentOperation != '':
                    continue
                print("run() last operation is finished")

            else:
                print('Please enter something that is valid')

if __name__ == '__main__':
    cmd = '-1'
    while True:
        cmd = raw_input('Do you want to initialize this VM to be master?\n Y/N')

        if cmd not in ['Y', 'N']:
            continue
        else:
            break

    mapper = {'1': UNKNOWN, '2' : GOSSIP, '3' : ALLTOALL}
    master_mapper = {'Y': True, 'N': False}

    gossiper = Talker(mapper['3'], master_mapper[cmd])
    gossiper.run()
