#!/bin/python


from parse import YarnParser
import getopt,sys
from os import listdir
from os.path import isfile,join

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
        opts,args=getopt.getopt(args,None,['yarn=','output=','usage'])
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
            print yarn_dir
        elif o == "--output":
            output_dir=v
        else:
            assert False, "unhandled option"
            print_usage()
            return False
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
            files=files+traver_dirs(file_path)
    return files
                

if __name__=="__main__":

    if parse_usage(sys.argv[1:]) is False:
        sys.exit()
    ##traver all subdir to log path
    if logs_dir is None:
        copy_logs()
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
        elif "nodemanager" in f:
            nm_logs.append(f)
        elif: "stderr" in f:
            app_logs.append(f)
        else:
            pass
    ##initialize parser
    yarn_parser=YarnParser(rm_log,nm_logs,app_logs)
    yarn_parser.rm_parse()
    yarn_parser.nm_parse()
    yarn_parser.sort_by_time()
    
      
     
