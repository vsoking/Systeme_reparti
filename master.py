import subprocess
from sys import stderr, stdin

usr_name = "vsoking-20"
machine_prefix = "tp-4b01-"
machine_num = ['39', '40', '41', '42']
exec = "slave.py"
remote_dir_path = "/tmp/" + usr_name + "/"
process_list = []

def usr_hostname(n):
    return usr_name + '@' + machine_prefix + n

# create directory and copy executable
for num in machine_num:
    subprocess.call(['ssh', usr_hostname(num), "mkdir -p ", remote_dir_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    subprocess.call(['scp', exec, usr_hostname(num)+":"+remote_dir_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE)



for num in machine_num:
    proc = subprocess.Popen(['ssh', usr_hostname(num), "python", remote_dir_path+exec], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    process_list.append(proc)


for ps in process_list:
    out , err  = ps.communicate()
    code = ps.returncode
    print("stdout:", out)
    print("stderr:", err)
    print("return code:", code)