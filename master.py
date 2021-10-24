from functools import reduce
from io import SEEK_END, SEEK_SET
import subprocess
import warnings
import time
import ast
import argparse
machine_list = ["vsoking-20@tp-4b01-"+"%02d" % i for i in range(20,23)]
#sleep_time_list = ["%02d" % 1 for i in range(39,42)]
#timeout_list = [3 for i in range(39,41)]
exec = "slave.py"
remote_dir_path = "/tmp/vsoking-20"
process_list = []
thread_list = []
script_name = "/tmp/vsoking-20/slave.py" 


def run_func(ps):
    out, err = ps.communicate()
    code = ps.returncode
    print("out: ", out)
    print("err:", err)

def create_dir(dir_name, slaves):
    proc_list = []
    r = 0
    for s in slaves:
        p = subprocess.Popen(['ssh', s, "mkdir -p ", dir_name], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} to create dir: '{}".format(out, err, p.returncode, s))   


def deploy(slaves_file, dir):
    proc_list = []
    r = 0
    for s_f in slaves_file:
        p = subprocess.Popen(['scp',  s_f[1], s_f[0]+":"+dir], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves_file):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} unable to deploy on: '{}".format(out, err, p.returncode, s_f[0]))

def execute_map(slaves_files):
       
    proc_list = []
    r = 0
    for s_f in slaves_files:
        cmd = "python3 {} -m {} {}".format(script_name, s_f[1], 'UM'+s_f[1][1:])
        p = subprocess.Popen(['ssh',  s_f[0], cmd], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves_files):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} map error on: '{}".format(out, err, p.returncode, s_f[0]))

def execute_shuffle(slaves_files):
    proc_list = []
    r = 0
    for s_f in slaves_files:
        cmd = "python3 {} -s {} {}".format(script_name, 'UM'+s_f[1][1:], 'To_avoid_err')
        p = subprocess.Popen(['ssh',  s_f[0], cmd], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves_files):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} shuffle error on: '{}".format(out, err, p.returncode, s_f[0])) 

def execute_reduce(slaves):
    proc_list = []
    r = {}
    for s in slaves:
        cmd = "python3 {} -r {} {}".format(script_name, 'To_avoid_err', 'To_avoid_err')
        p = subprocess.Popen(['ssh',  s, cmd], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        out , err = p.communicate()
        out = out.decode("utf-8")
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} reduce error on: '{}".format(out, err, p.returncode, s))
        elif len(out):            
            out = ast.literal_eval(out)
            r.update(out)
    return r



def get_connected_slaves(slaves):
    proc_list = []
    slaves_ok = []
    for s in slaves:
        p = subprocess.Popen(['ssh', s, "hostname"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)

    for s, p in zip(slaves, proc_list):
        p.communicate()
        if not p.returncode:
            slaves_ok.append(s)
    return slaves_ok

 
      

def clean(slaves):
    proc_list = []
    r = 0    
    for s in slaves:
        p = subprocess.Popen(['ssh', s, "rm -rf ", remote_dir_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        p.communicate()
        if p.returncode:
            warnings.warn("error '{} unable to clean: '{}".format(p.returncode, s))
        


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="input file")
    args = parser.parse_args()
    if args.input:
        m = get_connected_slaves(machine_list)

        start_time = time.perf_counter()
        create_dir(remote_dir_path, m)
        split_dir = remote_dir_path+"/splits"
        create_dir(split_dir, m)

        d_list = [(s, "slave.py") for s in m]
        deploy(d_list, remote_dir_path)
        
        #create list of all files name
        file_list = ["S{}.txt".format(i) for i in range(len(m))]

        
        in_f = open(args.input, 'r')
        in_f.seek(0, SEEK_END)
        split_size = (in_f.tell() // len(m)) + 1
        in_f.seek(0, SEEK_SET)
        for f in file_list:
            split = open(f, "w")
            split.write(in_f.read(split_size))
            split.close()
        in_f.close()

        #create list of (slave_name, file_to_deploy, directory where file will be deploy)
        
        d_list = [(s, f) for s, f  in zip(m, file_list)]

        #copy files to remote machine
        deploy(d_list, split_dir)

        
        execute_map(d_list)
        map_duration = time.perf_counter() - start_time
        print("MAP FINISHED... {:.2f} seconds".format(map_duration) )

        m_file = open("machines.txt", 'w')
        m_file.write(" ".join(m))
        m_file.close()

        start_time = time.perf_counter()
        deploy([(s, "machines.txt") for s in m], remote_dir_path)
        create_dir("/tmp/vsoking-20/shufflesreceived", m)
        execute_shuffle(d_list)
        shuffle_duration = time.perf_counter() - start_time
        print("SHUFFLE FINISHED... {:.2f} seconds".format(shuffle_duration))

        start_time = time.perf_counter()
        r = execute_reduce(m)
        reduce_duration  = time.perf_counter() - start_time
        print("REDUCE FINISHED... {:.2f} seconds".format(reduce_duration))
        print("TOTAL DURATION: ", map_duration + shuffle_duration + reduce_duration)
        r_file_name = args.input+"-wordcount-result.txt"
        r_file = open(r_file_name, 'w')
        r_file.write(str(r))
        r_file.close()
        print("RESULTS in file: {}".format(r_file_name))

        clean(m)


if __name__ == "__main__":
    main()

"""
for machine, t in zip(machine_list, sleep_time_list):
    proc = subprocess.Popen(['ssh', usr_hostname(machine), "python", remote_dir_path+exec, str(t) ], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    thrd = Thread(target=run_func, args=(proc,), daemon=True)
    thread_list.append(thrd)
    process_list.append(proc)
    thrd.start()


for thrd, t , p in zip(thread_list, timeout_list, process_list):
    thrd.join()
    if thrd.is_alive():
        print("Timeout... Killing the process")
        p.kill()

#clean
for machine in machine_list:
    subprocess.call(['ssh', usr_hostname(machine), "rm -rf ", remote_dir_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        
"""