#!/bin/python


class Analyze:

    """
    apps: map for app to events
    return successful apps without att failed
    """
    @staticmethod
    def success_apps(apps):
        success={}
        illegal=0
        for app,events in apps.items():
            failed=False
            sub_time=0
            driver_ini=0
            driver_reg=0
            executor_ass=0
            first=True
            for event in events:
                if event.source == "RM_ATT" and event.state == "FAILED":
                    failed = True
                    break
                elif event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "INIT":
                    driver_ini=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "REG":
                    driver_reg=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "ASS" and first:
                    executor_ass=event.time
                    first=False

                else:
                    pass
            if (driver_ini - sub_time >0 and driver_reg - driver_ini>0 and executor_ass - driver_reg>0) is False:
                failed=True

            if failed is False:
                success[app]=events
            else:
                illegal = illegal + 1
        print "success illegal ",illegal
        return success
                



    """
    apps: map for app to events
    return the total delay fo all apps
    """
    @staticmethod
    def total_delay(apps):
        total_delays={}
        illegal=0
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time fo executor assigned
            assign_time=0
            total_delay=0
            ##only touch the fisrt container
            first=True
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "ASS" and first:
                    assign_time=event.time
                    first=False
                else:
                    pass
            if assign_time - sub_time > 0:
                total_delay = assign_time - sub_time
                total_delays[app]=total_delay
            else:
                illegal = illegal + 1
                print app,assign_time,sub_time
        print "total_delau illegal ",illegal
        return total_delays
   

    """
    events: events for an application
    return the fisrt assigned executor
    """
    @staticmethod
    def find_first_ass_executor(events):
        first = True
        for event in events:
            if event.source=="SPARK_EXECUTOR" and event.state == "ASS" and first:
                return event.id
        return None
 
    """
    apps: map for app to events
    return the out-applicatoon delay fo all apps
    """
    @staticmethod
    def out_application_delays(apps):
        out_delays={}
        illegal = 0
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time for driver init
            driver_ini=0
            ##time for driver register
            driver_reg=0
            ##time for executor init
            executor_ini=0
            ##time for executor assigned
            executor_ass=0
            ##first executor ass id
            executor_id =Analyze.find_first_ass_executor(events)
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "INIT":
                    driver_ini=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "REG":
                    driver_reg=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "INIT" and event.id == executor_id:
                    executor_int=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "ASS" and event.id == executor_id:
                    executor_ass=event.time
                else:
                    pass
            if (executor_ass - sub_time > 0) and (driver_reg - driver_ini > 0) and(executor_ass - executor_ini > 0):
                out_delays[app]=(executor_ass - sub_time) - (driver_reg - driver_ini) - (executor_ass - executor_int)
            else:
                illegal = illegal + 1
        print "out delays illegal ",illegal
        return out_delays


    """
    apps: map for app to events
    return the in-applicatoon delay fo all apps
    """
    @staticmethod
    def in_application_delays(apps):
        total_delays = Analyze.total_delay(apps)
        out_delays   = Analyze.out_application_delays(apps)
        in_delays={}
        illegal = 0
        for app in total_delays.keys():
            if out_delays.get(app) is not None:
                in_delay=total_delays[app] - out_delays[app]
                if in_delay > 0:
                    in_delays[app] = in_delay
                else:
                    illegal = illegal + 1
            else:
                illegal = illegal + 1
        print "in delays illegal ",illegal
        return in_delays


    




    """
    apps: map for app to events 
    return the am scheduling delay maps for all apps
    """
    @staticmethod
    def am_delay(apps):
        am_delays={}
        illegal = 0
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time fo am registered
            reg_time=0
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "RM_APP" and event.state == "RUNNING":
                    reg_time=event.time
                else:
                    pass
            if reg_time - sub_time > 0:
                am_delays[app]=(reg_time-sub_time)
            else:
                illegal = illegal + 1
        print "am delays illegal ",illegal
        return am_delays
    
    """
    apps: map for app to events 
    return the first container launching scheduling delay maps for all apps

    """
    @staticmethod
    def c1_delay(apps):
        c1_delays={}
        illegal = 0
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time fo am registered
            c1_time=0
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "NM_CON" and event.state == "RUNNING" and event.id == 2:
                    c1_time=event.time
                else:
                    pass
            if c1_time - sub_time > 0:
                c1_delays[app]=(c1_time - sub_time)
            else:
                illegal = illegal + 1
        print "c1 delays illegal",illegal
        return c1_delays
    
    """
    return last launched container index
    """
    @staticmethod
    def last_container(events):
        index=2
        for event in events:
            if event.source == "NM_CON" and event.state == "RUNNING":
                if event.id > index:
                    index = event.id
        return index 

    """
    return if continer in events is launched
    """
    @staticmethod
    def is_launched(events,container):
        for event in events:
            if (event.source == "SPARK_EXECUTOR" or event.source == "SPARK_DRIVER"
                ) and event.id == container:
                return True
        return False

    """
    apps: map for app to events 
    return the last container launching scheduling delay maps for all apps

    """
    @staticmethod
    def cl_delay(apps):
        cl_delays={}
        illegal=0
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time fo am registered
            cl_time=0
            ##id for last container
            cl_id = Analyze.last_container(events)
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "NM_CON" and event.state == "RUNNING" and event.id == cl_id:
                    cl_time=event.time
                else:
                    pass
            if cl_time - sub_time > 0:
                cl_delays[app]=(cl_time - sub_time)
            else:
                illegal = illegal + 1
        print "cl delays illegal ",illegal
        return cl_delays

    """
    apps: map for app to events 
    return rm allocation delay for each container
    This delay is extraced by only parsing rm logs,
    no need to consider the time drift
    """
    @staticmethod
    def rm_allo_delay(apps):
        rm_allo_delays={}
        illegal = 0
        for app,events in apps.items():
            ##mapping for allocation starting time
            alls_time={}
            for event in events:
                if event.source == "RM_CON" and event.state == "ALLOCATED":
                    alls_time[event.id]=event.time
                elif event.source == "RM_CON" and event.state == "ACQUIRED":
                    if alls_time.get(event.id) and Analyze.is_launched(events,event.id):
                        ##compute allocation delay
                        all_delay=event.time - alls_time[event.id]
                        all_id  =app+"-"+str(event.id)
                        rm_allo_delays[all_id]=all_delay
                    else:
                        ##some containers are allocated but never launched
                        illegal = illegal + 1
                else:
                    pass
        print "rm allo delay illegal ",illegal
        return rm_allo_delays

    """
    apps: map for app to events 
    return the driver scheduling delay
    (when driver do some initialization
    before register with RM)

    """
    @staticmethod
    def driver_sche_delay(apps):
        driver_sche_delays={}
        illegal = 0
        for app,events in apps.items():
            ##first logged driver time
            driver_ini=0
            ##when am register with rm
            driver_reg=0         
            for event in events:
                if event.source == "SPARK_DRIVER" and event.state == "INIT":
                    driver_ini=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "REG":
                    driver_reg=event.time
                else:
                    pass
            if driver_reg - driver_ini > 0:
                driver_sche_delays[app]=(driver_reg - driver_ini)
            else:
                illegal = illegal + 1
        print "driver sched delay illegal ",illegal
        return driver_sche_delays
    
    """
    apps: map for app to events 
    return the executor scheduling delay

    """
    @staticmethod
    def executor_sche_delay(apps):
        executor_sche_delays={}
        illegal = 0
        for app,events in apps.items():
            ##mapping for allocation starting time
            ini_times={}
            for event in events:
                if event.source == "SPARK_EXECUTOR" and event.state == "INIT":
                    ini_times[event.id]=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "ASS":
                    if ini_times.get(event.id):
                        ##compute allocation delay
                        sche_delay=event.time - ini_times[event.id]
                        appcon_id  =app+"-"+str(event.id)
                        if sche_delay > 0:
                            executor_sche_delays[appcon_id]=sche_delay
                        else:
                            illegal = illegal + 1
                    else:
                        pass
                else:
                    pass
        print "executor sche delays illegal ",illegal
        return executor_sche_delays

    """
    apps: map from app to event
    return the launching delay for each container
    """
    @staticmethod
    def container_launching_delay(apps):
        container_launch_delays={}
        illegal = 0
        for app,events in apps.items():
            ##mapping for allocation starting time
            run_times={}
            for event in events:
                if event.source == "NM_CON" and event.state == "CONTAINER_LAUNCHED":
                    run_times[event.id]=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state== "INIT":
                    if run_times.get(event.id):
                        ##compute allocation delay
                        launch_delay=event.time - run_times[event.id]
                        appcon_id   =app+"-"+str(event.id)
                        if launch_delay > 0:
                            container_launch_delays[appcon_id]=launch_delay
                        else:
                            illegal = illegal + 1
                    else:
                        pass
                else:
                    pass
        print "container launch delay ",illegal
        return container_launch_delays

    """
    return the proportion of group A over B which has same key
    A: key_value
    B: key_value
    """
    @staticmethod
    def A_over_B(A,B):
        a_over_bs={}
        for key in A.keys():
            if B.get(key) is not None:
                a_over_b = (A[key]*1.0)/(B[key]*1.0)
                if a_over_b > 0:
                    a_over_bs[key]=a_over_b
                else:
                    pass
            else:
                pass
        return a_over_bs

    
    """
    apps: map from app to event
    return the application runtime
    """
    @staticmethod
    def app_runtime(apps):
        app_runtimes={}
        illegal=0
        for app,events in apps.items():
        ##time for app submited
            sub_time=0
            ##time fo am registered
            end_time=0
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "RM_APP" and event.state == "FINISHED":
                    end_time=event.time
                else:
                    pass
            if end_time - sub_time > 0:
                app_runtimes[app]=(end_time-sub_time)
            else:
                illegal = illegal + 1
        print "app runtime illegal ",illegal
        return app_runtimes
    
   

    
        
     
