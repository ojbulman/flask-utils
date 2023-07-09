from queue import Queue, Empty
from threading import Thread,current_thread
from time import sleep, time
from random import randint
import uwsgi
import json

wid = uwsgi.worker_id()

q = Queue()
maxrun=None
result_retention = 3600
threads = []

def init(max_size=0, max_run=2, retention=3600):
    """ Initialise queue and start worker thread """

    global q, maxrun, result_retention
    maxrun=max_run
    result_retention=retention
    #print(wid,"Queue Initialised")

    q = Queue(maxsize=max_size)
    Thread(target=__worker, daemon=True).start()  # Start worker thread


def save_tasks(tasks):
    uwsgi.cache_del("tasks")
    uwsgi.cache_set("tasks",json.dumps(tasks))


def load_tasks():
    tasks = uwsgi.cache_get("tasks")

    if not tasks:
        tasks = {}
    else:
        tasks = json.loads(tasks)
    
    return tasks


def xload_tasks():
    tids = uwsgi.cache_get("tids")
    #print("Load:",tids)
    tasks = {}

    if not tids:
        tids = []
    else:
        tids = json.loads(tids)
    
    for tid in tids:
        task = uwsgi.cache_get(tid)
        if tid:
            tasks[tid] = json.loads(task)
    
    return tasks

def update_task(task_id,task):
    task_id = str(task_id)

    uwsgi.lock()
    tasks = load_tasks()
    #print("Updating task",task_id)
    tasks[task_id] = task
    save_tasks(tasks)
    uwsgi.unlock()

def update_state(task_id,state):
    task_id = str(task_id)
    task = {"name":"","state":state,"result":None}
    #print("State Change",task_id)

    uwsgi.lock()
    tasks = load_tasks()
    if task_id in  tasks:
        task = tasks.get(task_id)
        task['state'] = state
        
    uwsgi.unlock()

    update_task(task_id,task)


def delete_task(task_id):
    task_id = str(task_id)
    #print("Deleting task...")
    uwsgi.lock()
    tasks = load_tasks()
    del tasks[task_id]
    uwsgi.cache_del(task_id)
    save_tasks(tasks)
    uwsgi.unlock()
    #print("Deleted Task:",task_id)


def get_new_id():
    uwsgi.lock()
    tasks = load_tasks()
    task_id = str(int(time()))

    while task_id in tasks:
        sleep(randint(0,10)/10)
        task_id = str(int(time()))

    uwsgi.unlock()
    update_task(task_id,{"state":"ALLOCATED"})
    return task_id


def queue(func,*args, name="", **kwargs):
    """ Add funciton to queue with given arguments and return unique task id 
    Eg. queueing.queue(my_func,"A",3)
    """
    global q

    # Generate unique task id
    task_id = get_new_id()

    task = {"name":name,"state":"QUEUED","result":None}

    # If callable function, add to queue
    if callable(func):
        x = q.put((task_id,func,*args, kwargs))  # Blocks if queue full
        update_task(task_id, task)
        print("Task Queued:",task_id)
        return task_id
    else:
        return None


def get_task(task_id):
    tasks = load_tasks()
    return tasks.get(task_id)


def get_status(task_id):
    """ Get status of running task """
    task_id = str(task_id)
    tasks = load_tasks()
    status = "UNKNOWN"

    if task_id in tasks:
        status = tasks.get(task_id)['state']

    return status


def add_results(result):
    """ Add results of the given task id 
    To be callled from task function
    """

    task_id = current_thread().name
    tasks = load_tasks()

    if task_id in tasks:
        tasks[task_id]['result'] = result
        tasks[task_id]['state'] = "RESULT"
        update_task(task_id, tasks[task_id])
        #print("Added Results:", task_id)


def check_results(task_id):
    task_id = str(task_id)
    """ Check for results of the given task id 
    Return and delete results if found
    """

    tasks = load_tasks()

    result = ""
    if task_id in tasks:
        result = tasks[task_id].get('result')
        delete_task(task_id)

    return result
    

def __worker():
    """ Worker function to run queued functions """

    while True :
        __check_ended_tasks()
        tasks=load_tasks()

        #print()
        #for task in tasks:
            #print("Loaded:",task,tasks[task])
        
        num_running = len([k for k,v in tasks.items() if v['state']=="RUNNING"])
        #print("Running=",num_running)
        

        # If execution capacity available, get next queued task and start execution
        if num_running < maxrun:
            try:
                items = q.get(timeout=1)
                func = items[1]
                args = items[2:]

                t = Thread(target=func, args=args)
                threads.append(t)
                t.name = items[0]
                t.start()
                print("Task Started:",t.name)

                update_state(t.name,"RUNNING")



            # Ignore queue empty exceptions from q.get()
            except Empty:
                pass

        sleep(5)


def __check_ended_tasks():
    """ Check running task threads & remove from threads list once complete """

    tasks = load_tasks()

    for process in threads:
        if not process.is_alive() and str(process.name) in tasks:
            if "state" in tasks[process.name]:
                if tasks[process.name]['state'] == "RUNNING":
                    print("Task Ended:",process.name)
                    update_state(process.name,"ENDED")
            #else:
                #print("No State:",tasks[process.name])
    
    # Delete old uncollected results    
    for tid, task in tasks.copy().items():
        if (time()-float(tid)) > result_retention:
            delete_task(tid)


# Sample: Queue new Task
# def queue_task():
#     from random import randint
#     num = randint(10,60)
#     tid = que.queue(test,num,name=f"Task {num}")

# Sample Task Function. Note that *args must be defined as the final parameter
# def test(num, *args):
#     for i in range(num):
#         print(wid,i,"/",num)
#         sleep(1)
    
#     print("Task Ended")
#     que.add_results(f"Task {num} Successful")