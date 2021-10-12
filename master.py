import subprocess
from sys import stderr, stdin
from threading import Thread

usr_name = "vsoking-20"
machine_prefix = "tp-4b01-"
machine_list = ['39', '40']#, '41', '42']
sleep_time_list = ['10', '10']
timeout_list = [2, 15]
exec = "slave.py"
remote_dir_path = "/tmp/" + usr_name + "/"
process_list = []
thread_list = []

def usr_hostname(n):
    return usr_name + '@' + machine_prefix + n

def run_func(ps):
    out, err = ps.communicate()
    code = ps.returncode
    print("out: ", out)
    print("err:", err)

# create directory and copy executable
for machine in machine_list:
    subprocess.call(['ssh', usr_hostname(machine), "mkdir -p ", remote_dir_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.call(['scp', exec, usr_hostname(machine)+":"+remote_dir_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE)



for machine, t in zip(machine_list, sleep_time_list):
    proc = subprocess.Popen(['ssh', usr_hostname(machine), "python", remote_dir_path+exec, str(t) ], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    thrd = Thread(target=run_func, args=(proc,), daemon=True)
    thread_list.append(thrd)
    process_list.append(proc)
    thrd.start()


for thrd, t , p in zip(thread_list, timeout_list, process_list):
    thrd.join(t)
    if thrd.is_alive():
        print("Timeout... Killing the process")
        p.kill()
        
