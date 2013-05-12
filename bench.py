import os
import sys
import subprocess
import time
import signal
from datetime import datetime
from os import system as system

def clear_buffer_cache():
    system('free -g')
    system('sync')
    system("sudo sed -n 's/0/3/w /proc/sys/vm/drop_caches' /proc/sys/vm/drop_caches")
    system('sync')
    system("sudo sed -n 's/3/0/w /proc/sys/vm/drop_caches' /proc/sys/vm/drop_caches")
    system('free -g')

def run(bin_path, args):
    clear_buffer_cache()
    clear_buffer_cache()

    now = datetime.now().strftime('%Y%m%d%H%M')
    test_name = '%s_%s_%s' %(now, args['--db'], args['--benchmarks'])
    report_name = '%s_report.txt' %test_name
    report_file = open(report_name, 'w')

    log_name = "%s_log.txt" %test_name
    logf = open(log_name, 'w')

    arg_list = [bin_path]
    for k, v in args.items():
        arg_list.append('%s=%s' %(k, v))

    print 'Run command %s' %(' '.join(arg_list))
    bench_process = subprocess.Popen(arg_list, stdout=report_file, stderr=logf)

    iostat_fname = '%s_iostat.txt' %test_name
    iostat_process = subprocess.Popen('iostat -xm 10 > %s' %iostat_fname,
                                      shell=True,
                                      preexec_fn=os.setsid)

    bench_process.wait()
    report_file.close()
    logf.close()
    os.killpg(iostat_process.pid, signal.SIGTERM)

    system("mkdir %s" %test_name)
    system("mv *.txt %s" %test_name)

def beans_read_write(db, value_size, thread_num, read_write_ratio):
    args =  {'--db':db,
             '--benchmarks':'readwhilewriting',
             '--value_size': value_size,
             '--num':'5000000',
             '--num_threads': thread_num,
             '--reads':'100000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':16,
             '--read_write_ratio': read_write_ratio}
    run('./db_bench', args)

def fs_read_write(db, value_size, thread_num, read_write_ratio):
    args =  {'--db':db,
             '--benchmarks':'readwhilewriting',
             '--value_size': value_size,
             '--num':'5000000',
             '--num_threads': thread_num,
             '--reads':'100000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5,
             '--read_write_ratio': read_write_ratio}
    run('./db_bench', args)

def beans_read(db, value_size, thread_num):
    args =  {'--db':db,
             '--benchmarks':'read',
             '--value_size': value_size,
             '--num':'5000000',
             '--num_threads': thread_num,
             '--reads':'1000000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':16,
             '--read_write_ratio': 1}
    run('./db_bench', args)

def fs_write(value_size, thread_num):
    # 500G
    args =  {'--db': 'fdb',
             '--benchmarks':'write',
             '--value_size': value_size,
             '--num':'5000000',
             '--num_threads': thread_num,
             '--reads':'100000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5}
    run('./db_bench', args)

def fs_read(value_size, thread_num):
    # 500G
    args =  {'--db': 'fdb',
             '--benchmarks':'read',
             '--value_size': value_size,
             '--num':'5000000',
             '--num_threads': thread_num,
             '--reads':'1000000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5}
    run('./db_bench', args)

def fs():
    value_size = int(100e3)
    fs_write(value_size, 4)
    fs_read(value_size, 4)

def read_write(db):
    # 100KB
    value_size = int(100e3)
    beans_read_write(db, value_size, 2, 1)
    beans_read_write(db, value_size, 6, 5)
    beans_read_write(db, value_size, 6, 0.2)

    # 1MB
    value_size = int(1e6)
    beans_read_write(db, value_size, 2, 1)
    beans_read_write(db, value_size, 6, 5)
    beans_read_write(db, value_size, 6, 0.2)

    # 5MB
    value_size = int(5e6)
    beans_read_write(db, value_size, 2, 1)
    beans_read_write(db, value_size, 6, 5)
    beans_read_write(db, value_size, 6, 0.2)

def fs_read_write_case(db):
    # 100KB
    value_size = int(100e3)
    fs_read_write(db, value_size, 2, 1)
    fs_read_write(db, value_size, 6, 5)
    fs_read_write(db, value_size, 6, 0.2)

    # 1MB
    value_size = int(1e6)
    fs_read_write(db, value_size, 2, 1)
    fs_read_write(db, value_size, 6, 5)
    fs_read_write(db, value_size, 6, 0.2)

    # 5MB
    value_size = int(5e6)
    fs_read_write(db, value_size, 2, 1)
    fs_read_write(db, value_size, 6, 5)
    fs_read_write(db, value_size, 6, 0.2)


def main():
    if sys.argv[1] == 'beans_read':
        beans_read('beansdb', int(100e3), 4)
    elif sys.argv[1] == 'fs':
        value_size = int(100e3)
        fs_read(value_size, 4)
        fs_read_write_case('fdb')
    else:
        print 'Error'

if __name__ == '__main__':
    main()

