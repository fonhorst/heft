#!D:\wspace\heft\penv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'pip==1.4.1','console_scripts','pip-3.3'
__requires__ = 'pip==1.4.1'
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.exit(
        load_entry_point('pip==1.4.1', 'console_scripts', 'pip-3.3')()
    )
