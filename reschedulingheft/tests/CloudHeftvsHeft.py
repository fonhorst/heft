import math
from reschedulingheft.tests.HeftExecutorExample import main as mainHeft
from reschedulingheft.tests.CloudHeftExecutorExample import main as mainCloudHeft
## Single fire
#main()

def run(run_name, mainFunc, wf_name):
    n = 100
    result = [mainFunc(True, wf_name) for i in range(n)]
    mx_time = max(result)
    min_time = min(result)
    avr_time = sum(result)/n
    avr_dispersion = math.sqrt(sum([math.pow(abs(res - avr_time), 2) for res in result]))
    print("==============common results: " + run_name + " " + wf_name + "================")
    print("           Max: " + str(mx_time))
    print("           Min: " + str(min_time))
    print("           Avr: " + str(avr_time))
    print("     Mean diff: " + str(avr_dispersion))
    return (avr_time,)

wf_name = "CyberShake_30"
##================Run Heft than CloudHeft
resHeft = run("Heft", mainHeft, wf_name)
resCloudHeft = run("CloudHeft", mainCloudHeft, wf_name)
print("===========================")
pc = (resHeft[0]/resCloudHeft[0] - 1)*100
print("cloudheft vs heft: " + str(pc))
print("===========================")




