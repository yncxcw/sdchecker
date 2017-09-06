#!/bin/python


class Analyze:


    """
    apps: map for app to events
    return the total delay fo all apps
    """
    @staticmethod
    def total_delay(apps)
        total_delays={}
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time fo executor assigned
            asign_time=0
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "ASS":
                    assign_time=event.time
                else:
                    pass
            if assign_time - sub_time > 0:
                total_delays[app]=(assign_time-sub_time)
            else:
                pass

        return total_delays
    
    """
    apps: map for app to events
    return the out-applicatoon delay fo all apps
    """
    @staticmethod(apps)
    def out_application_delays(apps):
        out_delays={}
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
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "INIT":
                    driver_ini=event.time
                elif event.source == "SPARK_DRIVER" and event.state == "REG":
                    driver_reg=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "INIT":
                    executor_int=event.time
                elif event.source == "SPARK_EXECUTOR" and event.state == "ASS":
                    executor_ass=event.time
                elif:
                    pass
            if (executor_ass - sub_time > 0) and (driver_reg - driver_ini > 0) and(executor_ass - executor_ini > 0):
                out_delays[app]=(executor_ass - sub_time) - (driver_reg - driver_ini) - (executor_ass - executor_int)
            else:
                pass

        return out_delays


    """
    apps: map for app to events
    return the in-applicatoon delay fo all apps
    """
    @staticmethod(apps)
    def out_application_delays(apps):
        total_delays = total_delay(apps)
        out_delays = out_application_delays(app)
        in_delays={}
        for app in total_delays.keys():
            if out_delays.get(app) is not None:
                in_delay=total_delays[app] - out_delays[app]
                if in_delay > 0:
                    in_delays[app] = in_delay
                else:
                    pass
            else:
                pass
        return in_delays


    




    """
    apps: map for app to events 
    return the am scheduling delay maps for all apps
    """
    @staticmethod
    def am_delay(apps):
        am_delays={}
        for app,events in apps.items():
            ##time for app submited
            sub_time=0
            ##time fo am registered
            reg_time=0
            for event in events:
                if event.source == "RM_APP" and event.state == "SUBMITTED":
                    sub_time=event.time
                elif event.source == "RM_ATT" and event.state == "RUNNING":
                    reg_time=event.time
                else:
                    pass
            if reg_time - sub_time > 0:
                am_delays[app]=(reg_time-sub_time)
            else:
                pass

        return am_delays
    
    """
    apps: map for app to events 
    return the first container launching scheduling delay maps for all apps

    """
    @staticmethod
    def c1_delay(apps):
        c1_delays={}
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
                pass
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
                    c1_time=event.time
                else:
                    pass
            if cl_time - sub_time > 0:
                cl_delays[app]=(cl_time - sub_time)
            else:
                pass
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
                        print "miss match event"
                else:
                    pass
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
                pass
        return driver_sche_delays
    
    """
    apps: map for app to events 
    return the executor scheduling delay

    """
    @staticmethod
    def executor_sche_delay(apps):
        executor_sche_delays={}
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
                        executor_sche_delays[appcon_id]=sche_delay
                    else:
                        print "miss match event"
                else:
                    pass
        return executor_sche_delays

    """
    apps: map from app to event
    return the launching delay for each container
    """
    @staticmethod
    def container_launching_delay(apps):
        container_launch_delays={}
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
                        container_launch_delays[appcon_id]=launch_delay
                    else:
                        print "miss match event"
                else:
                    pass
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



    
        
     
