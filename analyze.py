#!/bin/python


class Anylyze:

    """
    apps: map for app to events 
    return the am scheduling delay maps for all apps
    """
    @staticmethod
    def am_delay(apps):
        am_delays={}
        for app,events in apps.items():
            ##time for app submited
            sub_time
            ##time fo am registered
            reg_time
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
            sub_time
            ##time fo am registered
            c1_time
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
    def last_container(events):
        index=2
        for event in events:
            if event.source == "NM_CON" and event.state == "RUNNING":
                if event.id > index:
                    index = event.di
        return index 

    """
    apps: map for app to events 
    return the last container launching scheduling delay maps for all apps

    """
    @staticmethod
    def cl_delay(apps):
        cl_delays={}
        for app,events in apps.items():
            ##time for app submited
            sub_time
            ##time fo am registered
            cl_time
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
                    if alls_time.get(evnet.id):
                        ##compute allocation delay
                        all_dely=event.time - alls_time[event.id]
                        all_id  =app+"-"+str(event.id)
                        rm_allo_delays[all_id]=all_delay
                    else:
                        print "miss match event"
                else:
                    pass
        return rm_allo_delays


        
        
        
