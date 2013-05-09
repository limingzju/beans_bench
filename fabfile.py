from fabric.api import *
from fabric.state import output

env.sudo_prefix='sudo -iu dfs '

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
