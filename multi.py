from multiprocessing import Process, Queue, JoinableQueue, cpu_count
import time
import sys, os
import pandas
import sysconfig

class Consumer(Process):
    def __init__(self, task_queue, join_queue):
        Process.__init__(self)
        self.task_queue = task_queue
        self.reults_queue = join_queue

    def run(self):
        proc_name = self.name
        while True:
            task = self.task_queue.get()
            if task is None:
                print(f'Exciting process {proc_name}')
                self.task_queue.task_done()
                break
            print(f'{proc_name}, {task}')
            answer = task()
            self.task_queue.task_done()
            self.reults_queue.put(answer)
        return

class Task():

    def __init__(self, a, b):
        self.a = a
        self.b = b
        
    def __call__(self):
        time.sleep(0.1)
        return self.a, self.b

    def __str__(self):
        return f'{self.a} {self.b}'


if __name__ == '__main__':
    tasks_queue = JoinableQueue()
    results_queue = Queue()

    num_proc = cpu_count() * 2
    num_tasks = 20

    workers = [Consumer(tasks_queue, results_queue) for _ in range(num_proc)]
    
    for w in workers:
        w.start()

    for i in range(num_tasks):
        tasks_queue.put(Task(i, i))\
    
    for i in range(num_proc):
        tasks_queue.put(None)

    tasks_queue.join()

    for w in workers:
        w.join()

    for i in range(num_proc):
        result = results_queue.get()
        print(f'Result: {num_proc}:{result}')