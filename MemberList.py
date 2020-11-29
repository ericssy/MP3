from Util import *
import logging
import scpHelper

LOGGER = logging.getLogger("main")

class MemberList(object):

    def __str__(self):
        retStr = ''
        for id, content in self.members.iteritems():
            retStr += id + ':' + str(content) + '\n'
        return retStr
    def __init__(self, talker):
        super(MemberList, self).__init__()
        self.ip = talker.ip
        self.talker = talker
        self.id = talker.id
        self.members = dict()

        if not self.isIntroducer():
# I always add introducer to my membershiplist
# new grad means I just joined, I dont know what spreadtype others are using, Introducer will tell me what spreadtype
# I need to use if I define him as new grad
            self.members[INTRODUCER_IP] = {
                HEARTBEAT: 0,
                STATUS: NEW_GRAD,
                TIMESTAMP: 0
            }
            self.members[self.talker.id] = {
                HEARTBEAT: talker.heartbeat,
                STATUS: JOINED,
                TIMESTAMP: time.time()
            }
        else:
            self.members[INTRODUCER_IP] = {
                HEARTBEAT: talker.heartbeat,
                STATUS: JOINED,
                TIMESTAMP: time.time()
            }

    def isIntroducer(self):
        if self.ip == INTRODUCER_IP:
            return True
        return False

    def checkIpExist(self, ip):
        for id in self.members:
            if id.split('_')[0] == ip:
                return True
        return False

    def getLargestIp(self):
        max_ip_int = -1
        max_ip = ""
        for id in self.members:
            curr_ip_int = int(id.split('_')[0].replace('.',''))
            if curr_ip_int > max_ip_int:
                max_ip_int = curr_ip_int
                max_ip = id.split('_')[0]
        return max_ip

    def merge(self, rumor):

        for id, rumorContent in rumor.iteritems():
            if id == self.id:
                continue

            if rumorContent[STATUS] == 'JOINED':
                # This is the first time I heard message from him, I just add his rumor content in my dict
                if id not in self.members:
                    self.members[id] = rumorContent
                    logging.info('[JOINED] New VM with id %s just joined : \n' %  (id))
                    print('[JOINED] New VM with id %s just joined : \n' %  (id))
                else:
                    # if I heard old message from him, I dont update, if yes, I do
                    myHearBeatForThisIp = self.members[id][HEARTBEAT]
                    rumorHearBeatForThisIp = rumorContent[HEARTBEAT]
                    if rumorHearBeatForThisIp > myHearBeatForThisIp:
                        self.members[id] = rumorContent

            elif rumorContent[STATUS] == POTENTIAL_FAIL:
                # if I have already deleted him, or I have already flaged him as potential fail, I just contineu
                if id not in self.members or self.members[id][STATUS] == POTENTIAL_FAIL:
                    continue
                else:
                    # if I have never heard from his potential leave, then I will know he might be down
                    myHearBeatForThisIp = self.members[id][HEARTBEAT]
                    rumorHearBeatForThisIp = rumorContent[HEARTBEAT]

                    if rumorHearBeatForThisIp > myHearBeatForThisIp:
                        logging.info('[POTENTIAL_FAIL] Someone told me VM with id %s has an potential fail : \n' % (id))
                        print('[POTENTIAL_FAIL] Someone told me VM with id %s has an potential fail : \n' % (id))
                        self.members[id] = rumorContent
            # I heard he is leaving voluntary, I will update his status
            elif rumorContent[STATUS] == TO_QUIT:
                if id in self.members and self.members[id][STATUS] == JOINED:
                    self.members[id] = rumorContent
                    logging.info('[TO_QUIT_VOLUNTARY] I heard VM with id %s is going to quit : \n' % (id))
                    print('[TO_QUIT_VOLUNTARY] I heard VM with id %s is going to quit : \n' % (id))
                else:
                    continue
            # update timestamp for my local info for this ip
            myTimeStampForThisIp = self.members[id][TIMESTAMP]
            rumorTimeStampForThisIp = rumorContent[TIMESTAMP]

            if rumorTimeStampForThisIp > myTimeStampForThisIp:
                self.members[id][TIMESTAMP] = rumorTimeStampForThisIp

    def refresh(self):
        # I havent hear message about you, I will set you as potential leave, If even longer, I think you failed
        failedVm = []
        for id, content in self.members.items():
            if content[STATUS] == NEW_GRAD:
                continue
            # longTimeNoSee means how long I havent hear message that talks about you
            longTimeNoSee = time.time() - content[TIMESTAMP]
            if content[STATUS] == 'JOINED' and longTimeNoSee > float(POTENTIAL_FAIL_INTERVAL):
                logging.info('[POTENTIAL_FAIL]  I just detected VM with id %s has an potential fail : \n' % (id))
                print('[POTENTIAL_FAIL]  I just detected VM with id %s has an potential fail : \n' % (id))
                content[STATUS] = POTENTIAL_FAIL
            elif content[STATUS] == POTENTIAL_FAIL and longTimeNoSee > float(FAIL_INTERVAL):
                content[STATUS] = FAIL
                failedVm.append(id.split('_')[0])
                del self.members[id]
                logging.info('[FAIL] I just detected VM with id %s has failed : \n' % (id))
                print('[FAIL] I just detected VM with id %s has failed : \n' % (id))
        # if introducer is down, and I have already deleted it, then I need to change the status of Intro to NEWGRAD
        if (INTRODUCER_IP not in self.members and not self.isIntroducer()) :
            self.members[INTRODUCER_IP] = {
                HEARTBEAT: 0,
                STATUS: NEW_GRAD,
                TIMESTAMP: 0
            }
        return failedVm
    # everytime I send out a message to others, I will clean my list if my memberlist is changing to TO_QUIT
    def toQuitRefresh(self):
        for id, content in self.members.items():
            if content[STATUS] == TO_QUIT and id != INTRODUCER_IP:
                del self.members[id]

    def talkableMembers(self):
        ret = []
        for id, content in self.members.iteritems():
            if (self.isIntroducer() and self.ip == id) or (not self.isIntroducer() and self.id == id):
                continue
            if content[STATUS] == JOINED:
                ret.append(id.split('_')[0])
            if content[STATUS] == NEW_GRAD:
                ret.append(id.split('_')[0])

        return ret

    def rumorGeneration(self):
        # introducer has different id with others, introducer id is always the IP address
        # other IDs is IP + 'time.time()'
        if not self.isIntroducer():
            self.members[self.talker.id][HEARTBEAT] = self.talker.heartbeat
            self.members[self.talker.id][STATUS] = self.talker.status
            self.members[self.talker.id][TIMESTAMP] = self.talker.timestamp
        else:
            self.members[INTRODUCER_IP][HEARTBEAT] = self.talker.heartbeat
            self.members[INTRODUCER_IP][STATUS] = self.talker.status
            self.members[INTRODUCER_IP][TIMESTAMP] = self.talker.timestamp

        return self.members

