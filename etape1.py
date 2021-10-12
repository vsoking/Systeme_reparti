import sys

def main():
    f = open("input.txt", "r")
    s = f.read()
    d = {}
    for w in s.split():
        d[w] = s.count(w)
    print(d)
    d_sort = sorted(d.items(), key = lambda x: x[1], reverse=True)
    print(d_sort)

if __name__ == "__main__":
    sys.exit(main())