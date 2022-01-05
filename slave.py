
import argparse
import subprocess
from platform import node
import hashlib
import os
from collections import defaultdict
import json
import sys

host_name = node()
shuffle_received_dir_name = "shufflesreceived"
work_dir_path = "/tmp/vsoking-20/"
map_dir_name = "maps"

def create_local_dir(dir_name):
    r = 0
    p = subprocess.Popen(["mkdir",'-p', dir_name], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    out , err = p.communicate()
    if p.returncode:
        raise RuntimeError("Failed to create {} directory".format(dir_name))

def create_remote_dir(slaves, dir_name):
    proc_list = []
    r = 0
    for s in slaves:
        p = subprocess.Popen(['ssh', s, "mkdir -p ", dir_name], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_list.append(p)
    for p, s in zip(proc_list, slaves):
        out , err = p.communicate()
        if p.returncode:
            print(slaves)
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
            raise RuntimeError("{} {} error {} unable to deploy on: {}".format(out, err, p.returncode, s_f[0]))


def map(input_file, output_file):
    in_f = open("/tmp/vsoking-20/splits/"+input_file, 'r')
    #create_local_dir("/tmp/vsoking-20/maps/")
    os.makedirs("/tmp/vsoking-20/maps/", exist_ok=True)
    out_f = open("/tmp/vsoking-20/maps/"+output_file, 'w')
    s = in_f.read()
    d = {}
    for w in s.split():
        out_f.write("{} 1\n".format(w))
    in_f.close()
    out_f.close()

def shuffle(input_file):
    shuffle_dir = "/tmp/vsoking-20/shuffles"
    os.makedirs(shuffle_dir, exist_ok=True)
    #create_local_dir(shuffle_dir)
    in_f = open(work_dir_path+"maps/"+input_file, 'r')
    m_f = open("/tmp/vsoking-20/machines.txt")
    m_list = m_f.read().split(" ")
    host_id = m_list.index("vsoking-20@"+host_name)
    hash_files_dict = {i: defaultdict(list) for i in m_list} #{k:[] for k in m_list}
    mask = 0
    
    #slaves_files_list = []
    for line in in_f:
        s = bytes(line.split(" ")[0], "utf-8")
        hash_ = int.from_bytes(hashlib.md5(s).digest()[:4], 'big')        
        dest = hash_ % len(m_list)
        #out_file_name = str(hash_) +'-'+host_name +"-"+str(dest)+".txt"
        mask |= 1 << dest
        hash_files_dict[m_list[dest]][line.split()[0]].append(1)

    for i in range(len(m_list)):
        if mask & (1<<i):
            with open(shuffle_dir+"/"+host_name+'_'+m_list[i], "w") as f:
                #f.write(hash_files_dict[m_list[i]].getvalue())
                json.dump(hash_files_dict[m_list[i]], f)
    proc_list = []
    for i in range(len(m_list)):
        if mask & (1<<i):
            files_to_send = "/tmp/vsoking-20/shuffles/*_{}".format(m_list[i])
            remote_host = m_list[i]
            cmd = "scp "+files_to_send+" "+remote_host+":"+work_dir_path+shuffle_received_dir_name
            p = subprocess.Popen(cmd, shell=True ,stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
            proc_list.append(p)
    for p in proc_list:
        out , err = p.communicate()
        if p.returncode:
            raise RuntimeError("{} {} error {} unable to deploy ".format(out, err, p.returncode))               

    in_f.close()
    m_f.close()

def reduce():
    count_dict = defaultdict(int)
    for shuffle_file_name in os.listdir(work_dir_path+shuffle_received_dir_name):
        reduce_file_name = shuffle_file_name.split('-')[0]
        with open(work_dir_path+shuffle_received_dir_name+"/"+shuffle_file_name, "r") as f:
            d = json.load(f)
            for w in d:
                count_dict[w] += len(d[w])

    reduce_dir_path = "/tmp/vsoking-20/reduces"
    os.makedirs(reduce_dir_path, exist_ok=True)
    out_f = open(reduce_dir_path+"/result-"+host_name+".txt", 'w')
    json.dump(count_dict, out_f)
    out_f.close()
    """
    reduce_dir_path = "/tmp/vsoking-20/reduces"
    os.makedirs(reduce_dir_path, exist_ok=True)
    out_f = open(reduce_dir_path+"/result-"+host_name+".txt", 'w')
    for words_count in count_dict.items():
        out_f.write(words_count[0]+" "+str(words_count[1]) +'\n')
    out_f.close()
    """
    #return json.dumps(count_dict)

        


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-m", "--map", help="perfom map operation", nargs=2, action='store')
    group.add_argument("-r", "--reduce", help="perfom reduce operation", action="store_true")
    group.add_argument("-s", "--shuffle", help="perform shuffle operation", action='store')
    #parser.add_argument("input", help="input file")
    #parser.add_argument("output", help="output file")
    args = parser.parse_args()
    if args.map:
        map(args.map[0], args.map[1])
    elif args.shuffle:
        shuffle(args.shuffle)
    elif args.reduce:
        if os.path.isdir("/tmp/vsoking-20/shufflesreceived"):
            reduce()
        else:
            pass
    else:
        pass

if __name__ == "__main__":
    main()