#!/bin/python

import re
import json
from matcher import * 


start=-1

#yarn log parser
class YarnParser:


    
    ##log file for rm and log file list for nm_logs
    def __init__(self,rm_log,nm_logs):
        self.rm_log =rm_log
        self.nm_logs=nm_logs
        self.apps={}

   
    def add_event(self,app,event):
        if app is not None:
            if self.apps.get(app) is None:
                self.apps[app]=[]
            self.apps[app].append(event)
            return True
        else:
            return False 

    def rm_parse(self):
        rm_file=open(self.rm_log,"r")
        for line in rm_file.readlines():
            app,event=RM_app_matcher.try_to_match(line)
            if self.add_event(app,event):    
                continue


            app,event=RM_att_matcher.try_to_match(line)
            if self.add_event(app,event):
                continue

            app,event=RM_con_matcher.try_to_match(line)
            if self.add_event(app,event):
                continue
                
            


    def nm_parse(self):
        for f in self.nm_logs:
            nm_file=open(f,"r")
            for line in nm_file.readlines():
                app,event=NM_con_matcher.try_to_match(line)
                if self.add_event(app,event):
                    continue

    ##TODO analyze the allocation delay and launching dey
    def spark_parse(self):
        pass

    ##sort evetns in each app by time stamp
    def sort_by_time(self):
        for app in self.apps.keys():
            app_events=self.apps[app]
            app_events.sort(key=lambda x: x.time)
        with open("./test.data","w") as outfile:
            for app in self.apps.keys():
                outfile.write(app+"\n")
                for event in self.apps[app]:
                    outfile.write(str(event)+"\n") 

    def get_apps(self):
        return self.apps
            

