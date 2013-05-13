import os
import sys

standard = '/mnt/dfs/1/dbtest/0000000000004235'
def Remove(path):
    if os.path.isfile(path):
        sz = os.path.getsize(path)
        if sz != 100000:
            os.system('cp %s %s' %(standard, path))
            print 'cp %s %s' %(standard, path)
    else:
        files = os.listdir(path)
        for f in files:
            full_name = os.path.join(path, f)        
            Remove(full_name)

if __name__ == '__main__':
    Remove(sys.argv[1])
