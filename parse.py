#!/bin/python

import re
import json
import os
from matcher import * 
#import cPickle

start=-1

#yarn log parser
class YarnParser:    
    ##log file for rm and log file list for nm_logs
    def __init__(self,rm_log,nm_logs,spark_app_logs,mapreduce_app_logs):
        self.rm_log =rm_log
        self.nm_logs=nm_logs
        self.spark_app_logs=spark_app_logs
        self.mapreduce_app_logs=mapreduce_app_logs
        self.apps={}
        ##help information recording if a executor is logged first ASS
        self.apps_ass={}
   
    def add_event(self,app,event):
        if app is None:
            return False;
        if event is None:
            return False
        if self.apps.get(app) is None:
            self.apps[app]=[]
        self.apps[app].append(event)
        return True

    def rm_parse(self):
        assert(self.rm_log is not None)
        rm_file=open(self.rm_log,"r")
        host = self.rm_log.split("-")[-1].split(".")[0]
        for line in rm_file.readlines():
            app,event=RM_app_matcher.try_to_match(line,host)
            if self.add_event(app,event):    
                continue


            app,event=RM_att_matcher.try_to_match(line,host)
            if self.add_event(app,event):
                continue

            app,event=RM_con_matcher.try_to_match(line,host)
            if self.add_event(app,event):
                continue
                
    def nm_parse(self):
        assert(self.nm_logs > 0)
        for f in self.nm_logs:
            nm_file=open(f,"r")
            host = f.split("-")[-1].split(".")[0]
            for line in nm_file.readlines():
                app,event=NM_con_matcher.try_to_match(line,host)
                if self.add_event(app,event):
                    continue

    def mapreduce_parse(self):
        if len(self.mapreduce_app_logs) == 0:
            return
        for f in self.mapreduce_app_logs:
            app_file=open(f,"r")
            host="null"
            for line in app_file.readlines():
                terms=f.split("/")[-2].split("_")
                ##we can only extract app id from file path 
                ##TODO add host
                app_name=terms[1]+"_"+terms[2]
                ##for executor log, we need additionly extract
                ##container id
                container_id=int(terms[4])
                if container_id == 1:
                    app,event=MAPREDUCE_master_matcher.try_to_match(line,"None","None")
                    if self.add_event(app_name,event):
                        continue
                else:
                    app,event=MAPREDUCE_slave_matcher.try_to_match(line,"None",container_id)
                    if self.add_event(app_name,event):
                        continue 
        pass





    ##TODO analyze the allocation delay and launching dey
    def spark_parse(self):
        if len(self.spark_app_logs) == 0:
            return
        for f in self.spark_app_logs:
            app_file=open(f,"r")
            host="null"
            for line in app_file.readlines():
                terms=f.split("/")[-2].split("_")
                ##we can only extract app id from file path 
                ##TODO add host
                app_name=terms[1]+"_"+terms[2]
                ##for executor log, we need additionly extract
                ##container id
                container_id=int(terms[4])
                if container_id == 1:
                    app,event=SPARK_master_matcher.try_to_match(line,"None","None")
                    if self.add_event(app_name,event):
                        continue
                else:
                    app,event=SPARK_slave_matcher.try_to_match(line,"None",container_id)
                    ##we only need first matched ASS
                    if event is None:
                        continue

                    if event.state == "ASS" and self.log_first_ass(app_name,container_id) is False:
                        continue
                         
                    if self.add_event(app_name,event):
                        continue 
        pass

    ##return if this ass event is first logged for app_name and container_id
    def log_first_ass(self,app_name,container_id):
        if self.apps_ass.get(app_name) is None:
            self.apps_ass[app_name]=set()
            self.apps_ass[app_name].add(container_id)
            return True
        if container_id in self.apps_ass[app_name]:
            return False
        else:
            self.apps_ass[app_name].add(container_id)
            return True

    ##sort evetns in each app by time stamp
    def sort_by_time(self):
        for app in self.apps.keys():
            app_events=self.apps[app]
            app_events.sort(key=lambda x: x.time)
        self.serialize()

    def serialize(self):
        with open("./data.tmp","w") as outfile:
            for app in self.apps.keys():
                outfile.write("appid "+app+"\n")
                for event in self.apps[app]:
                    outfile.write(str(event)+"\n")

    def deserialize(self):
        with open("./data.tmp","r") as inputfile:
            self.apps={}
            appid=None
            for line in inputfile.readlines():
                if "appid" in line:
                    appid=line.split()[1]
                    self.apps[appid]=[]
                else:
                    items=line.split()
                    time  =int(items[0])
                    source=items[1]
                    id    =int(items[2])
                    state =items[3]
                    eve   =items[4]
                    host  =items[5]
                    event =Event(time,source,id,state,eve,host)
                    self.apps[appid].append(event)
                    
                     
    def get_apps(self):
        if os.path.isfile("./data.tmp"):
            self.deserialize()
        return self.apps
            

