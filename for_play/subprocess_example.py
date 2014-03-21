import subprocess

# args = ['ssh', '-x', '-n', '-oStrictHostKeyChecking=no', '192.168.168.130', '(export PYTHONPATH=D:\\wspace\\heft:$PYTHONPATH >&/dev/null || setenv PYTHONPATH D:\\wspace\\heft:$PYTHONPATH && C:\\Pro\\Python33\\python.exe -m scoop.bootstrap.__main__ --echoGroup  --size 5 --workingDirectory D:\\wspace\\heft\\for_play --brokerHostname fonhorst-C2Q --externalBrokerHostname fonhorst-C2Q --taskPort 63354 --metaPort 54507 --backend=ZMQ -v D:/wspace/heft/for_play/full_tree.py)']
import sys

args = ['ssh', '-x', '-n', '-oStrictHostKeyChecking=no', 'user@192.168.168.130', "(echo 'Hello World' > /home/user/hw.test.txt)"]
# args = ['-x', '-n', '-oStrictHostKeyChecking=no', 'user@192.168.168.130', "(echo 'Hello World' > /home/user/hw.test.txt)"]
out, err = subprocess.Popen(args,
                 # executable="ssh",
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True
                ).communicate()
print(str(sys.getdefaultencoding()))
print("=============Out print==================")
print(out.decode("utf-8"))
print("=============Err print==================")
print(err.decode("utf-8"))

