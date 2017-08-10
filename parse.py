#!/bin/python

import re
from match import * 


start=-1

#yarn log parser
class YarnPaser:


    
    ##log file for rm and log file list for nm_logs
    def __init__(rm_log,nm_logs):
        self.rm_log =rm_log
        self.nm_logs=nm_logs
        self.apps={}

    

    def rm_parse():
        rm_file=open(self.rm_log,"r")
        for line in rm_file.readlines():
            app,event=RM_att_matcher.try_to_match(line)
            if app is not None:
                self.apps[app]=event
                continue
            app,event=RM_app_matcher.try_to_match(line)

            if app is not None:
                self.apps[app]=event
                continue
            app,event=RM_con_matcher.try_to_match(line)

            if app is not None:
                self.apps[app]=event
                continue
                
            


    def nm_parse():

