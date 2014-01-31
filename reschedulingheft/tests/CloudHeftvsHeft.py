from datetime import datetime
import json
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

    return (mx_time, min_time, avr_time)

def get_dict(result):
    res = dict()
    res['Max'] = result[0]
    res['Min'] = result[1]
    res['Avr'] = result[2]
    return res

wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
            "Montage_25", "Montage_50", "Montage_75", "Montage_100",
            "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
            "Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100",
            "Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]
# wf_names = [
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_100"]

#wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_100"]
#wf_names = ["Montage_25", "Montage_50", "Montage_100"]
# wf_names = ["Montage_25", "Montage_50"]
# wf_names = ["Epigenomics_24", "Epigenomics_46"]
# wf_names = ["Inspiral_30", "Inspiral_50"]
# wf_names = ["Sipht_30", "Sipht_60"]

#wf_names = ["Epigenomics_24", "Epigenomics_46", "Epigenomics_100"]
#wf_names = ["Inspiral_30", "Inspiral_50", "Inspiral_100"]
#wf_names = ["Sipht_30", "Sipht_60", "Sipht_100"]
# wf_names = ["Sipht_30"]

# wf_names = ['CyberShake_30']

common_time = datetime.now().strftime("%d_%m_%y %H_%M_%S")
path = '..\\..\\resources\\saved_simulation_results\\' + 'HeftVsCloudHeft_' + common_time + '.json'
path_for_gnuplot = '..\\..\\resources\\saved_simulation_results\\' + 'HeftVsCloudHeft_' + common_time + '.txt'
##================Run Heft than CloudHeft
def HeftVsCloudHeft(wf_name):
    print("Calculating now - " + wf_name)
    resHeft = run("Heft", mainHeft, wf_name)
    resCloudHeft = run("CloudHeft", mainCloudHeft, wf_name)
    print("===========================")
    pc = (1 - resCloudHeft[2]/resHeft[2])*100
    print("cloudheft vs heft: " + str(pc))
    print("===========================")

    result = dict()
    result['wf_name'] = wf_name
    result['heft'] = get_dict(resHeft)
    result['cloud_heft'] = get_dict(resCloudHeft)
    result['profit_by_avr'] = pc

    f = open(path, 'a')
    json.dump(result, f)
    f.close()

    g = open(path_for_gnuplot, 'a')
    heft = "" + "_heft" + " " + str(resHeft[2]) + " " + wf_name + '\n'
    cheft = "" + "cheft" + " " + str(resCloudHeft[2]) + " " + wf_name + '\n'
    g.writelines([heft, cheft])
    g.close()



[HeftVsCloudHeft(wf_name) for wf_name in wf_names]





