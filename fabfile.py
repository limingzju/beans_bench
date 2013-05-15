from fabric.api import *
from fabric.state import output

env.sudo_prefix='sudo -iu dfs '

@hosts('hzliming2013@inspur113.photo.163.org')
def upload(*args):
    fname = args[0]
    put(fname, '~/')
    orignal = '/home/styxhome/hzliming2013/'
    run('sudo -iu dfs cp %s/%s /mnt/dfs/1/dbtest' %(orignal, fname))

@hosts('hzliming2013@inspur112.photo.163.org')
def compile():
    run('rm ~/beans_bench.tar.gz')
    run('rm -rf  ~/beans_bench')
    run('rm -rf ~/build')
    local('cd .. && tar cvzf beans_bench.tar.gz beans_bench')
    put('../beans_bench.tar.gz', '~/')

    run('tar xvzf beans_bench.tar.gz')
    run('mkdir build')
    with cd('build'):
        run('~/local/bin/cmake ../beans_bench && make')
    get('~/build/db_bench', '.')

@hosts('hzliming2013@inspur113.photo.163.org')
def upload113():
    run('sudo -iu dfs mkdir -p /mnt/dfs/1/backup')
    #run('sudo -iu dfs mv /mnt/dfs/1/{bench.py,db_bench} /mnt/dfs/1/backup')
    upload('bench.py', '.')
    upload('db_bench', '.')
    home = '/home/styxhome/hzliming2013/'
    run('sudo -iu dfs cp %s/bench.py /mnt/dfs/1/' %home)
    run('sudo -iu dfs cp %s/db_bench /mnt/dfs/1/' %home)
    run('sudo -iu dfs chmod a+x /mnt/dfs/1/bench.py')
    run('sudo -iu dfs chmod a+x /mnt/dfs/1/db_bench')

@hosts('hzliming2013@inspur114.photo.163.org')
def upload114():
    dest = '/mnt/dfs/10/liming'
    run('sudo -iu dfs mkdir -p %s/backup' %dest)
    upload('bench.py', '.')
    upload('db_bench', '.')
    home='/home/styxhome/hzliming2013/'
    run('sudo -iu dfs cp %s/bench.py %s' %(home, dest))
    run('sudo -iu dfs cp %s/db_bench %s' %(home, dest))
    run('sudo -iu dfs chmod a+x %s/bench.py' %dest)
    run('sudo -iu dfs chmod a+x %s/db_bench' %dest)


@hosts('hzliming2013@inspur112.photo.163.org')
def test():
    print env
    print env.sudo_prompt
    print env.sudo_prefix
    print env.sudo_prefix % env

    output.debug=True
#    run("sudo -iu dfs ls")
#    sudo('ls', shell=False, user='dfs')
    sudo('ls', user='dfs')
    put('./crc32.c', '/mnt/dfs/10', use_sudo=True)
#    sudo('touch /mnt/dfs/10/test.txt', shell=True, user='dfs')
