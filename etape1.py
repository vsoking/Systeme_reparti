import sys
import pandas as pd
import time
from collections import defaultdict

def main():
    start_time = time.perf_counter()
    f = open(sys.argv[1], "r")
    s = f.read()
    d = defaultdict(int)
    for w in s.split():
        d[w] += 1
    total = time.perf_counter() - start_time
    print("TOTAL DURATION... ", total)

    r_file_name = "wordcount-seq-"+sys.argv[1]
    df = pd.Series(d).to_csv(r_file_name, header=None)
    print("RESULTS in file: {}".format(r_file_name))
    f.close()

if __name__ == "__main__":
    sys.exit(main())