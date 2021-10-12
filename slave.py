import time
import sys

def main(argv):
    time.sleep(int(argv[0]))
    #print("slave slept for: %d and  calculate 3+5= %d" %(int(argv[0]),3+5))
    sys.stderr.write("slave slept for: %d and  calculate 3+5= %d" %(int(argv[0]),3+5))

if __name__ == "__main__":
    main(sys.argv[1:])