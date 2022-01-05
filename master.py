from io import SEEK_END, SEEK_SET
from platform import system
import subprocess
import warnings
import time
import pandas as pd
import json
import argparse
import threading
import os

machine_list = ["vsoking-20@tp-1a207-"+"%02d" % i for i in range(1,40)]
exec = "slave.py"
remote_dir_path = "/tmp/vsoking-20"
process_list = []
thread_list = []
script_name = "/tmp/vsoking-20/slave.py" 

def ping_slave(slave):
    if (0 != os.system("ping -c 1 "+ slave.name)):
        slave.is_alive = False
    time.sleep(3)

class Slave:
    def __init__(self, name):
        self.name = name
        #self.is_alive = True
        self.phase = ''
        self.user = "vsoking-20"
        #threading.Thread(target=ping_slave, args=(self,))
    
    def execute(self, cmd):
        return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)



def create_dir(dir_name, slave_list):
    proc_list = []
    for slave in slave_list:
        usr_host = slave.user+'@'+slave.name
        cmd = "ssh {} mkdir -p {}".format(usr_host, dir_name)
        p = slave.execute(cmd)
        proc_list.append(p)
    for p, s in zip(proc_list, slave_list):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} to create dir: '{}".format(out, err, p.returncode, s.name))   


def deploy(slaves_file, dir):
    proc_list = []
    r = 0
    for s_f in slaves_file:
        cmd = "scp {} {}".format(s_f[1], s_f[0].user+'@'+s_f[0].name+":"+dir)
        p = s_f[0].execute(cmd)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves_file):
        #while s[0].is_alive:
        #    try: 
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} unable to deploy on: '{}".format(out, err, p.returncode, s_f[0].name))
        #        break
        #    except subprocess.TimeoutExpired:
        #        pass
        #else:
        #    print("{} is not alive".format(s[0].name))

def execute_map(slaves_files):       
    proc_list = []
    r = 0
    for s_f in slaves_files:
        cmd = "ssh {} python3 {} -m {} {}".format(s_f[0].user+'@'+s_f[0].name, script_name, s_f[1], 'UM'+s_f[1][1:])
        p = s_f[0].execute(cmd)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves_files):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} map error on: '{}".format(out, err, p.returncode, s[0].name))


def execute_shuffle(slaves_files):
    proc_list = []
    r = 0
    for s_f in slaves_files:
        cmd = "ssh {} python3 {} -s {}".format(s_f[0].user+'@'+s_f[0].name, script_name, 'UM'+s_f[1][1:])
        p = s_f[0].execute(cmd)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves_files):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} shuffle error on: '{}".format(out, err, p.returncode, s[0].name))
                



def execute_reduce(slaves):
    proc_list = []

    for s in slaves:
        cmd = "ssh {} python3 {} -r ".format(s.user+'@'+s.name, script_name)
        p = s.execute(cmd)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} reduce error on: '{}".format(out, err, p.returncode, s.name))

def get_result(slaves):
    proc_list = []
    r = {}
    out_dir = "reduces"
    os.makedirs(out_dir, exist_ok=True)
    for s in slaves:
        cmd = "scp {}:/tmp/vsoking-20/reduces/* {}/".format(s.user+'@'+s.name, out_dir)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        out, err = p.communicate()
        if p.returncode:
            raise RuntimeError("'{} '{} error '{} reduce error on: '{}".format(out, err, p.returncode, s.name))

    for reduce_file in os.listdir(out_dir):
        with open(out_dir+'/'+reduce_file, "r") as f:
            d = json.load(f)
            r.update(d)
    subprocess.call(["rm", "-rf", out_dir])
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
            p.kill()
    return slaves_ok      

def clean(slaves):
    proc_list = []
    r = 0    
    for s in slaves:
        cmd = "ssh {} rm -rf {}".format(s.user+'@'+s.name, remote_dir_path)
        p = s.execute(cmd)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        p.communicate()
        if p.returncode:
            warnings.warn("error '{} unable to clean: '{}".format(p.returncode, s.name))


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="input file")
    parser.add_argument("-m", "--workers", help="number of workers", type=int, default=3)
    args = parser.parse_args()
    if args.input:
        start_time = time.perf_counter()        
        m = get_connected_slaves(machine_list)
        check_connect_duration = time.perf_counter() - start_time
        print("Checking connected slaves... {} seconds".format(check_connect_duration))

        if args.workers > len(m):
            raise ValueError("only {} worker available".format(len(m)))
        m = m[:args.workers]
        slave_list = [Slave(i.split("@")[1]) for i in m]

        print("list of slaves {}".format([s.name for s in slave_list]))
        
        start_time = time.perf_counter()  
        create_dir(remote_dir_path, slave_list)
        split_dir = remote_dir_path+"/splits"
        create_dir(split_dir, slave_list)

        d_list = [(s, "slave.py") for s in slave_list]
        deploy(d_list, remote_dir_path)

        
        #create list of all files name
        file_list = ["S%02d"%i for i in range(len(slave_list))]

        
        subprocess.call("split -n l/{} -a 2 -d {} S".format(len(slave_list), args.input), shell=True)

        #create list of (slave_name, file_to_deploy, directory where file will be deploy)
        
        d_list = list(zip(slave_list, file_list))

        #copy files to remote machine
        deploy(d_list, split_dir)
        deploy_duration = time.perf_counter() - start_time
        print("DEPLOY  SPLIT FINISHED... {} seconds".format(deploy_duration))

        start_time = time.perf_counter()
        execute_map(d_list)
        map_duration = time.perf_counter() - start_time
        print("MAP FINISHED... {:.2f} seconds".format(map_duration) )

        m_file = open("machines.txt", 'w')
        m_file.write(" ".join(m))
        m_file.close()

        
        deploy([(s, "machines.txt") for s in slave_list], remote_dir_path)
        create_dir("/tmp/vsoking-20/shufflesreceived", slave_list)
        start_time = time.perf_counter()
        execute_shuffle(d_list)
        shuffle_duration = time.perf_counter() - start_time
        print("SHUFFLE FINISHED... {:.2f} seconds".format(shuffle_duration))

        start_time = time.perf_counter()
        execute_reduce(slave_list)        
        reduce_duration  = time.perf_counter() - start_time
        print("REDUCE FINISHED... {:.2f} seconds".format(reduce_duration))

        start_time = time.perf_counter()
        r = get_result(slave_list)
        r_file_name = "wordcount-parallel-"+args.input
        pd.Series(r).to_csv(r_file_name, header=None)
        combine_duration = time.perf_counter() - start_time
        print("COMBINE RESULTS... {} seconds".format(combine_duration))

        print("TOTAL DURATION: ", map_duration + shuffle_duration + reduce_duration)
        print("RESULTS in file: {}".format(r_file_name))
        
        clean(slave_list)


if __name__ == "__main__":
    main()