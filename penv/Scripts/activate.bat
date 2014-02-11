@echo offset VIRTUAL_ENV=D:\wspace\heft\penvif not defined PROMPT (    set PROMPT=$P$G)if defined _OLD_VIRTUAL_PROMPT (    set PROMPT=%_OLD_VIRTUAL_PROMPT%)if defined _OLD_VIRTUAL_PYTHONHOME (     set PYTHONHOME=%_OLD_VIRTUAL_PYTHONHOME%)set _OLD_VIRTUAL_PROMPT=%PROMPT%set PROMPT=(penv) %PROMPT%if defined PYTHONHOME (     set _OLD_VIRTUAL_PYTHONHOME=%PYTHONHOME%     set PYTHONHOME=)

if defined PYTHONPATH (
	set _OLD_PYTHONPATH=%PYTHONPATH%
)
set PYTHONPATH=D:\wspace\heft;D:\wspace\heft\penv\Lib\site-packages; 

if defined _OLD_VIRTUAL_PATH set PATH=%_OLD_VIRTUAL_PATH%; goto SKIPPATHset _OLD_VIRTUAL_PATH=%PATH%:SKIPPATHset PATH=%VIRTUAL_ENV%\Scripts;%PATH%:END