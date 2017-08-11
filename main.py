#!/bin/python


from parse import YarnParser
import getopt,sys
from os import listdir
from os.path import isfile,join

##input yarn logs dir
yarn_dir=None
##output result logs dir
output_dir=None

def print_usage():
   print """
          usage for this tool
         

         """ 

def parse_usage(args):
    global yarn_dir
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
        elif o == "--yarn":
            yarn_dir=v
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

if __name__=="__main__":

    if parse_usage(sys.argv[1:]) is False:
        sys.exit()
    yarn_files=[join(yarn_dir,f) for f in listdir(yarn_dir) if isfile(join(yarn_dir,f))]
    rm_log=None
    nm_logs=[]
    ##iterate files
    for f in yarn_files:
        if "resourcemanager" in f:
            rm_log=f
        elif "nodemanager" in f:
            nm_logs.append(f)
        else:
            pass
    ##initialize parser
    yarn_parser=YarnParser(rm_log,nm_logs)
    yarn_parser.rm_parse()
    yarn_parser.nm_parse()
    yarn_parser.sort_by_time()
    
      
     
