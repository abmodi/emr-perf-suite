#!/usr/bin/env python

import subprocess;
import Queue
import threading
import time
from optparse import OptionParser

class AsyncProcessPoll(threading.Thread):
    def __init__(self, process):
        self.process = process
        threading.Thread.__init__(self)
        self._retcode = None

    def run(self):
        while self.process.poll() is None:
            time.sleep(1)
            self._retcode = self.process.returncode

    def get_return_code(self):
        return self._retcode


class AsynchronousFileReader(threading.Thread):
    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
 
    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue
 
    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)
        
    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()

def main():
    optparser = OptionParser()
    optparser.add_option("--file", dest="querFile", default=None, help="Query File")
    (options, args) = optparser.parse_args()
    shCmd = "/usr/bin/hive -f " + options.querFile + ";"
    process = subprocess.Popen(shCmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    poll_thread = AsyncProcessPoll(process)
    poll_thread.daemon=True
    poll_thread.start()
    # Launch the asynchronous readers of the process' stdout and stderr.

    stderr_queue = Queue.Queue()
    stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
    stderr_reader.daemon=True
    stderr_reader.start()

    while not stderr_reader.eof():
        # Show what we received from standard error.
        while not stderr_queue.empty():
            line = stderr_queue.get()
            print "stderr: " + line


    stderr_queue.join()
    # Close subprocess' file descriptors.
    process.stdout.close()
    process.stderr.close()
    poll_thread.join()
    print poll_thread.get_return_code()
    return poll_thread.get_return_code()

main()