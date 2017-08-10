#!/bin/python

import re

class Matcher:

    @staticmethod
    def try_to_match(line):
        pass

class RM_att_matcher(Matcher):
    ##                                                                                   appid attid       
    rm_att = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+)RMAppAttemptImpl (\S+)_(\S+)_(\d+) State change from (\w+) to (\w+) on event = (\S+)')

    @staticmethod
    def try_to_match(line):
        match =rm_att.match(line)
        if match:
            groups=match.groups()
            time =int(groups[2])*3600*1000+int(groups[3])*1000+int(groups[4])
            app  =groups[7]
            ##used to count sometimes
            attid=int(groups[8])
            new  =groups[10]
            eve  =groups[11]
            event=Event(time,"RM_ATT",attid,new,eve)
            return app,event
        else:
            return None,None
            
            
        
class RM_app_matcher(Matcher):

    ##match state transfer in rm RMAppImpl                                        appid 
    rm_app = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+)RMAppImpl (\S+)_(\S+) State change from (\w+) to (\w+) on event = (\S+)')

    @staticmethod
    def try_to_match(line):
        match=rm_app.match(line)
        if match:
            groups=match.groups()
            time  =int(groups[2])*3600*1000+int(groups[3])*1000+int(groups[4])
            app   =groups[7]
            appid =int(app.split("_")[1])
            new   =groups[9]
            eve   =groups[10]
            event =Event(time,"RM_APP",appid,new,eve)
            return app,event
        else:
            return None,None
        

class RM_con_matcher(Matcher):
   
    ##match state transfer in rm RMContainerImpl                         appid       conid  
    rm_con = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+) (\S+)_(\S+)_(\d+)_(\d+) Container Transitioned from (\w+) to (\w+)')
 
    @staticmethod
    def try_to_match(line): 
        match=rm_con.match(line)
        if match:
            groups=match.groups()
            time  =int(groups[2])*3600*1000+int(groups[3])*1000+int(groups[4])
            app   =groups[7]
            conid =int(groups[9])
            new   =groups[11]
            eve   =""
            event =Event(time,"RM_CON",conid,new,eve)
            return app,event
        else:
            return None,None
            

class Event:

                
    def __init__(time,source,id,state,eve)
        """
           time: time stamp, resolution is ms
           source: source of this event(e.g., rm_att)
           id: att id or container id
           state: state after the transformation
           eve:on what event trigers this transform, if it has one
        """
        self.time  = time
        self.source= source
        self.id    = id
        self.state = state
        self.eve   = eve



