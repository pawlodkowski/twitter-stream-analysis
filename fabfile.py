from fabric import task
from fabric import Connection
from subprocess import call

CMD = 'python3 twitter_stream_analysis/twitter_stream_analysis/load_database.py'
USER = 'ec2-user'
HOST = 'ec2-35-157-179-142.eu-central-1.compute.amazonaws.com'
c = Connection(
        user= USER,
        host= HOST,
        connect_kwargs={'key_filename': 'spicykeys.pem'}
    )

@task
def hello(context):
    print('Hello')
    #For testing the fabfile

@task
def install(context):
    c.run('sudo yum -y install python3') #sudo yum -y install python36 doesn't work, so use 3
    print("\nSuccessfully installed Python 3\n")
    c.run('sudo yum -y install git')
    print("\nSuccessfully installed git\n")
    c.run('git clone https://github.com/pawlodkowski/twitter-stream-analysis.git')
    print("Successfully cloned the github repository")
    c.run('pip3 install --user -r twitter-stream-analysis/requirements.txt')
    print("Successfully installed the requirements file")
    c.run('pip3 install --user -r twitter-stream-analysis/requirements_dev.txt')
    print("Successfully installed the requirements_dev file")
    call(['scp', '-qi', 'spicykeys.pem', 'twitter-stream-analysis/config.py', f'{USER}@{HOST}:twitter-stream-analysis/twitter-stream-analysis/config.py'])
    #c.copy('config.py', 'tweego/tweego/config.py') #this has to be manual because config.py is in the .gitignore; therefore, it needs to be copied over from the local machine via scp.
    print("Successfully transferred over the local config files")
    c.run('pip3 install --user -e tweego/') #this is needed because in the code, we use an import twitter-stream-analysis statement that presupposes a pip-installed twitter-stream-analysis version.
    print("Finished the installation!")
    #c.run(f'sudo echo "30 * * * * ec2-user {CMD}" >> /etc/crontab')


@task
def singlerun(context):
    c.run(f' {CMD}')

@task
def repeatrun(context):
    c.run(f'sudo echo "0,20,40 * * * * ec2-user {CMD}" >> /etc/crontab') #cron daemon to run jobs frequently. in this case at the 0, 20, and 40 minute mark of every hour
