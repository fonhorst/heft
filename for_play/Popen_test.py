import subprocess

sshcmd = ['ssh', '-x', '-n', '-oStrictHostKeyChecking=no', 'user@192.168.168.130', '(/usr/local/bin/python3.3 -m scoop.bootstrap.__main__ --echoGroup  --size 3 --workingDirectory /home/user/heft --brokerHostname 10.253.0.151 --externalBrokerHostname 10.253.0.151 --taskPort 63350 --metaPort 52547 --backend=ZMQ -v for_play/full_tree.py)']
fout = open("D:/stdout.txt", "wb")
ferr = open("D:/stderr.txt", "wb")
subprocess.Popen(sshcmd,
                                 stdout=fout,
                                 stderr=ferr,
                ).wait()
