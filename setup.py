from os import makedirs
from subprocess import Popen, PIPE

def run(cmd):
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    if stdout:
        print('STDOUT:\n%s\n' % (stdout.decode(),))
    if stderr:
        print('STDERR:\n%s\n' % (stderr.decode(),))
    if p.returncode != 0:
        raise('The following command failed: %s' % (cmd,))
  
run('mkdir -p flight-analytics/data')
run('curl -k https://ibis-resources.s3.amazonaws.com/data/airlines/airlines_parquet.tar.gz -o flight-analytics/data/airlines_parquet.tar.gz')
run('tar -C flight-analytics/data/ -xvzf flight-analytics/data/airlines_parquet.tar.gz')
run('hadoop fs -mkdir -p /tmp/airlines/')
run('hadoop fs -put -f flight-analytics/data/airlines_parquet/* /tmp/airlines/')

run('curl http://stat-computing.org/dataexpo/2009/airports.csv > flight-analytics/data/airports.csv')
run('hadoop fs -mkdir -p /tmp/airports')
run('hadoop fs -put -f flight-analytics/data/airports.csv /tmp/airports/')

run('hadoop fs -chmod 777 /tmp/airlines /tmp/airports')

run('rm -rf flight-analytics/data')

run('hadoop fs -ls -R /tmp/airlines /tmp/airports')

