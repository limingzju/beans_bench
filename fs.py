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

    now = datetime.now().strftime('%m%d%H%M%S')
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


def _fs_read_write(num, value_size, thread_num, read_write_ratio, read_num):
    args =  {'--db': 'fdb',
             '--benchmarks':'readwhilewriting',
             '--value_size': value_size,
             '--num': num,
             '--num_threads': thread_num,
             '--reads': read_num,
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5,
             '--read_write_ratio': read_write_ratio}
    run('./db_bench', args)


def _fs_write(num, value_size, thread_num):
    # 500G
    args =  {'--db': 'fdb',
             '--benchmarks':'write',
             '--value_size': value_size,
             '--num': num,
             '--num_threads': thread_num,
             '--reads':'100000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5}
    run('./db_bench', args)

def _fs_read(num, thread_num, read_num):
    # 500G
    args =  {'--db': 'fdb',
             '--benchmarks':'read',
             '--value_size': int(100e3),
             '--num': num,
             '--num_threads': thread_num,
             '--reads': read_num,
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5}
    run('./db_bench', args)


def _fs_read_case(num):
    # Test Threads (1, 4, 8 16)
    _fs_read(num, 1, 10000)
    _fs_read(num, 2, 10000)
    _fs_read(num, 4, 10000)
    _fs_read(num, 8, 10000)
    _fs_read(num, 16, 10000)

    # Test read_num
    _fs_read(num, 4, 1000)
    _fs_read(num, 4, 2000)
    _fs_read(num, 4, 5000)

def _fs_write_case(num):
    """ Write data 4 threads totally 500GB data """
    print '_fs_write_case'
    thread_num = 4
    value_size = int(100e3)
    read_write_ratio = 1
    args =  {'--db': 'fdb',
             '--benchmarks':'write',
             '--value_size': value_size,
             '--num': num,
             '--num_threads': thread_num,
             '--reads':'100000',
             '--use_existing_db':'1',
             '--histogram':'1',
             '--filesystem_ndir':5,
             '--read_write_ratio': read_write_ratio}
    run('./db_bench', args)


def _fs_read_write_case(num):
    print '_beans_read_write_case'
    read_num = 10000
    db = 'beansdb'
     # 100KB
    value_size = int(100e3)
    print '100KB'
    _fs_read_write(num, value_size, 2, 1, read_num)
    _fs_read_write(num, value_size, 6, 5, read_num)
    _fs_read_write(num, value_size, 6, 0.2, read_num)

    # 1MB
    print '1MB'
    value_size = int(1e6)
    _fs_read_write(num, value_size, 2, 1, read_num)
    _fs_read_write(num, value_size, 6, 5, read_num)
    _fs_read_write(num, value_size, 6, 0.2, read_num)

    # 5MB
    print '5MB'
    value_size = int(5e6)
    _fs_read_write(num, value_size, 2, 1, read_num)
    _fs_read_write(num, value_size, 6, 5, read_num)
    _fs_read_write(num, value_size, 6, 0.2, read_num)

def main():
    if sys.argv[1] == 'fs':
        num = int(5000e3)
        #num = 1000
        _fs_write_case(num)
        _fs_read_case(num)
        _fs_read_write_case(num)

if __name__ == '__main__':
    main()

