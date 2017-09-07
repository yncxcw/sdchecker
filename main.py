#!/bin/python


from parse import YarnParser
import getopt,sys
from os import listdir
from os import mkdir
from os.path import isfile,join
import utils
from analyze import Analyze
import operator
##input yarn logs dir
log_dir=None
##output result logs dir
output_dir=None

def print_usage():
   print """
          usage for this tool
         

         """ 

def parse_usage(args):
    global log_dir
    global output_dir

    try:    
        opts,args=getopt.getopt(args,None,['logs=','output=','usage'])
    except getopt.GetoptError as err:
        print str(err)
        print_usage()
        return False
    for o, v in opts:
        if o == "--usage":
            print_usage()
            return False
        elif o == "--logs":
            log_dir=v
            print log_dir
        elif o == "--output":
            output_dir=v
        else:
            assert False, "unhandled option"
            print_usage()
            return False
    if output_dir is None:
        mkdir("output")
        output_dir="./output"
        print "will save results to ./output"
    if len(args) > 0:
        print "invalid args"
        print_usage()
        return False
    return True 

"""
recursively traverse logs under this folder,
results are stored in a file list
"""

def traverse_dirs(logs_dir):
    files=[]
    for f in listdir(logs_dir):
        file_path=join(logs_dir,f)
        if isfile(file_path):
            files.append(file_path)
        else:
            files=files+traverse_dirs(file_path)
    return files
   

def persist_map(file_name,key_values):
    ##sort based on values
    key_values=sorted(key_values.items(),key=lambda x:x[1])
    f=open(file_name,"w")
    for key_value in key_values:
        f.write(key_value[0]+"   "+str(key_value[1])+"\n")
                     

if __name__=="__main__":

    if parse_usage(sys.argv[1:]) is False:
        sys.exit()
    ##traver all subdir to log path
    if log_dir is None:
        #copy_logs()
        log_dir="./logs"
    files=traverse_dirs(log_dir)
    ##classify logs
    rm_log=None
    nm_logs=[]
    app_logs=[]
    ##iterate files
    for f in files:
        if "resourcemanager" in f:
            rm_log=f
            #print rm_log
        elif "nodemanager" in f:
            nm_logs.append(f)
            #print f
        elif "stderr" in f:
            app_logs.append(f)
            #print f
        else:
            pass
    ##initialize parser
    yarn_parser=YarnParser(rm_log,nm_logs,app_logs)
    ##parse logs
    #yarn_parser.rm_parse()
    #yarn_parser.nm_parse()
    #yarn_parser.spark_parse()
    ##sort by times
    #yarn_parser.sort_by_time()
    tapps=yarn_parser.get_apps()
    ##parse successful apps
    apps=Analyze.success_apps(tapps)
    ##do analysis
    total_delays=Analyze.total_delay(apps)
    persist_map(output_dir+"/total",total_delays)
    
    in_delays=Analyze.in_application_delays(apps)
    persist_map(output_dir+"/in",in_delays)

    out_delays=Analyze.out_application_delays(apps)
    persist_map(output_dir+"/out",out_delays) 

    am_delays=Analyze.am_delay(apps)
    persist_map(output_dir+"/am",am_delays)

    c1_delays=Analyze.c1_delay(apps)
    persist_map(output_dir+"/cf",c1_delays)

    cl_delays=Analyze.cl_delay(apps)
    persist_map(output_dir+"/cl",cl_delays)

    rm_allo_delays=Analyze.rm_allo_delay(apps)
    persist_map(output_dir+"/rm",rm_allo_delays)
     
    driver_sche_delays=Analyze.driver_sche_delay(apps)
    persist_map(output_dir+"/driver",driver_sche_delays)

    executor_sche_delays=Analyze.executor_sche_delay(apps)
    persist_map(output_dir+"/executor",executor_sche_delays)

    container_launching_delays=Analyze.container_launching_delay(apps)
    persist_map(output_dir+"/launching",container_launching_delays) 
    
    app_runtimes=Analyze.app_runtime(apps)
    persist_map(output_dir+"/appruntime",app_runtimes)  
     
