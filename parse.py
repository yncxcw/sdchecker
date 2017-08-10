#!/bin/python

import re
from match import * 


start=-1

#yarn log parser
class YarnParser:


    
    ##log file for rm and log file list for nm_logs
    def __init__(self,rm_log,nm_logs):
        self.rm_log =rm_log
        self.nm_logs=nm_logs
        self.apps={}

    

    def rm_parse(self):
        rm_file=open(self.rm_log,"r")
        for line in rm_file.readlines():
            app,event=RM_att_matcher.try_to_match(line)
            if app is not None:
                ##always first match the app event 
                if self.apps.get(app) is None:
                    self.apps[app]=[]
                self.apps[app].append(event)
                continue

            app,event=RM_app_matcher.try_to_match(line)
            if app is not None:
                self.apps[app].append(event)
                continue

            app,event=RM_con_matcher.try_to_match(line)
            if app is not None:
                self.apps[app].append(event)
                continue
                
            


    def nm_parse(self):
        for f in nm_logs:
            nm_file=open(f,"r")
            app,event=NM_con_matcher.try_to_match(line)
            if app is not None:
                self.apps[app].append(event)
                continue

    ##TODO analyze the allocation delay and launching dey
    def spark_parse(self):

    ##sort evetns in each app by time stamp
    def sort_by_time(self):
        for app in self.apps.keys():
            app_events=self.apps[app]
            app_events.sort(key=lambda x: x.time)

    def get_apps(self):
        return self.apps
            

