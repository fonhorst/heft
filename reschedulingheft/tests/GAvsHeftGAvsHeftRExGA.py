from datetime import datetime
from functools import partial
import json
import math
from environment.Utility import Utility
from reschedulingheft.tests.CloudHeftExecutorExample import main as mainCloudHeft
from reschedulingheft.tests.HeftExecutorExample import main as mainHeft
from reschedulingheft.tests.GAExecutorExample import main as mainGA
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
print("reliability 0.75")

wf_names = ['Inspiral_72']
# wf_names = ['Sipht_79']

# wf_names = ['CyberShake_75', 'Montage_75', 'Epigenomics_72', 'Inspiral_72', 'Sipht_73']

# wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
#             "Montage_25", "Montage_50", "Montage_75", "Montage_100",
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]

# wf_names = ["Sipht_100"]
# wf_names = ["Epigenomics_24"]
# wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_100",
#             "Montage_25", "Montage_50", "Montage_100",
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_100"]
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
#wf_names = ["Sipht_30"]

# wf_names = ["Epigenomics_100"]
# wf_names = ["Sipht_100"]
# wf_names = ["Inspiral_100"] #bad
# wf_names = ["Montage_100"] #bad
#wf_names = ["CyberShake_100"]
#wf_names = ["CyberShake_50"]
# wf_names = ["Montage_100"]
# wf_names = ["Inspiral_50"]
#wf_names = ["Epigenomics_46"]


# wf_names = ["CyberShake_30"]
# wf_names = ["CyberShake_50"]
# wf_names = ["CyberShake_100"]

# wf_names = ["Montage_25"]
# wf_names = ["Montage_50"]
# wf_names = ["Montage_100"]

# wf_names = ["Epigenomics_24"]
# wf_names = ["Epigenomics_46"]
# wf_names = ["Epigenomics_100"]

# wf_names = ["Inspiral_30"]
# wf_names = ["Inspiral_50"]
# wf_names = ["Inspiral_100"]

# wf_names = ["Sipht_30"]
# wf_names = ["Sipht_60"]
# wf_names = ["Sipht_100"]

#wf_name = "CyberShake_50"
#wf_name = "CyberShake_100"

common_time = datetime.now().strftime("%d_%m_%y %H_%M_%S")
save_path = '..\\..\\resources\\saved_simulation_results\\' + 'HeftGAVsGA_' + common_time + '.json'
path_for_gnuplot = '..\\..\\resources\\saved_simulation_results\\' + 'HeftGAVsGA_' + common_time + '.txt'
##================Run Heft than CloudHeft

def HeftVsCloudHeft(wf_name):

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000

    dax2 = '..\\..\\resources\\' + wf_name + '.xml'
    ## dedicated resource are the same for all bundles
    path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
    bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1))

    mainCloudHEFTwithGA = partial(mainCloudHeft, with_ga_initial=True, the_bundle=bundle)
    mainHEFTwithGA = partial(mainHeft, with_ga_initial=True, the_bundle=bundle)
    mainGAwithBundle = partial(mainGA, bundle=bundle)

    print("Calculating now - " + wf_name)
    resGA = run("GA", mainGAwithBundle, wf_name)
    resHeft = run("Heft + GA", mainHEFTwithGA, wf_name)
    resCloudHeft = run("HeftREx + GA", mainCloudHEFTwithGA, wf_name)
    print("===========================")
    pc_hg = (1 - resHeft[2]/resGA[2])*100
    pc_chg = (1 - resCloudHeft[2]/resGA[2])*100
    pc_chh = (1 - resCloudHeft[2]/resHeft[2])*100
    print("heft + ga vs ga: " + str(pc_hg))
    print("heftREx vs heft + ga: " + str(pc_chh))
    print("heftREx vs ga: " + str(pc_chg))
    print("===========================")

    result = dict()
    result['wf_name'] = wf_name
    result['heft'] = get_dict(resHeft)
    result['ga'] = get_dict(resGA)
    result['profit_by_avr'] = [pc_hg, pc_chh, pc_chg]

    f = open(save_path, 'a')
    json.dump(result, f)
    f.close()

    g = open(path_for_gnuplot, 'a')
    heft = "" + "__heftGa" + " " + str(resHeft[2]) + " " + wf_name + '\n'
    cheft = "" + "____ga" + " " + str(resGA[2]) + " " + wf_name + '\n'
    cheft = "" + "heftREx" + " " + str(resCloudHeft[2]) + " " + wf_name + '\n'
    g.writelines([heft, cheft])
    g.close()



[HeftVsCloudHeft(wf_name) for wf_name in wf_names]





