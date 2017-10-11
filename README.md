### SDchecker: A tool to check the scheduling delay for Apache YARN
SDchecker is log mining tool that we design to study the scheduling delay for applications run on Apache YARN. 


## How it works:
SDchecker mines bothe YARN and application logs (e.g., Spark) to contruct a scheduling graph. It further 
decompose each piece of the delay from the scheduling graph so that users can use it to study these delays. 
Currently, it supports tracing the scheduling delay for Spark on YARN, we plan to supprt other frameworks, like 
Hadoop, Tez in the future.

Currently it can report:

(1) total delay: the delay from job submission to first Spark task is scheduled to run

(1) AM delay: the delay from job submission to Application master laucnhing.

(2) launching delay: container launching delay.

(3) localization delay: container localization delay.

(4) driver delay: Spark driver initialization delay.

(5) executor delay: Spark executor initialization delay.  


## Usage:

python --logs /log-directiory  --output /output-directory

Note: users need to copy hadoop logs under the log-directiory from all slaves nodes, the hadoop logs is located at $HADOOP\_HOME/logs.

We welcome you comments on this project.
