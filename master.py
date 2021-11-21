from io import SEEK_END, SEEK_SET
import subprocess
import warnings
import time
import pandas as pd
import json
import argparse

machine_list = ["vsoking-20@tp-4b01-"+"%02d" % i for i in range(0,44)]
exec = "slave.py"
remote_dir_path = "/tmp/vsoking-20"
process_list = []
thread_list = []
script_name = "/tmp/vsoking-20/slave.py" 


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
        cmd = "python3 {} -s {}".format(script_name, 'UM'+s_f[1][1:])
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
        cmd = "python3 {} -r ".format(script_name)
        p = subprocess.Popen(['ssh',  s, cmd], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        out , err = p.communicate()
        out = out.decode("utf-8")
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} reduce error on: '{}".format(out, err, p.returncode, s))
        elif len(out):            
            out = json.loads(out)
            r.update(out)
    return r

def get_connected_slaves(slaves):
    proc_list = []
    slaves_ok = slaves
    for s in slaves:
        p = subprocess.Popen(['ssh', s, "hostname"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)

    for s, p in zip(slaves, proc_list):
        try:
            p.communicate(timeout=4)
        except subprocess.TimeoutExpired:
            slaves_ok.remove(s)
            pass
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
    parser.add_argument("-m", "--workers", help="number of workers", type=int, default=3)
    args = parser.parse_args()
    if args.input:
        m = get_connected_slaves(machine_list)
        if args.workers > len(m):
            raise ValueError("only {} worker available".format(len(m)))
        m = m[:args.workers]

        print("list of workers {}".format([i[-2:] for i in m]))
        
        create_dir(remote_dir_path, m)
        split_dir = remote_dir_path+"/splits"
        create_dir(split_dir, m)

        d_list = [(s, "slave.py") for s in m]
        deploy(d_list, remote_dir_path)
        
        #create list of all files name
        file_list = ["S{}.txt".format(i) for i in range(len(m))]

        start_time = time.perf_counter()
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
        r_file_name = "wordcount-"+args.input
        pd.Series(r).to_csv(r_file_name, header=None)
        print("RESULTS in file: {}".format(r_file_name))
        
        clean(m)


if __name__ == "__main__":
    main()