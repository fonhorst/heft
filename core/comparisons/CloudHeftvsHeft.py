import json
from core.comparisons.ComparisonBase import run, get_dict, save_path, path_for_gnuplot
from core.examples.CloudHeftExecutorExample import CloudHeftExecutorExample
from core.examples.HeftExecutorExample import HeftExecutorExample

## Single fire
#main()

mainHeft = HeftExecutorExample().main
mainCloudHeft = CloudHeftExecutorExample().main

##================Run Heft than CloudHeft
def HeftVsCloudHeft(wf_name, reliability):
    print("Calculating now - " + wf_name)
    resHeft = run("Heft", mainHeft, wf_name, reliability)
    resCloudHeft = run("CloudHeft", mainCloudHeft, wf_name, reliability)
    print("===========================")
    pc = (1 - resCloudHeft[2]/resHeft[2])*100
    print("cloudheft vs heft: " + str(pc))
    print("===========================")

    result = dict()
    result['wf_name'] = wf_name
    result['heft'] = get_dict(resHeft)
    result['cloud_heft'] = get_dict(resCloudHeft)
    result['profit_by_avr'] = pc

    f = open(save_path, 'a')
    json.dump(result, f)
    f.close()

    g = open(path_for_gnuplot, 'a')
    heft = "" + "_heft" + " " + str(resHeft[2]) + " " + wf_name + '\n'
    cheft = "" + "cheft" + " " + str(resCloudHeft[2]) + " " + wf_name + '\n'
    g.writelines([heft, cheft])
    g.close()
    pass


reliability = 0.95
print("reliability %s" % reliability)
wf_names = ["CyberShake_30"]
#wf_names = ['new_generated\\CyberShake_30','new_generated\\CyberShake_50','new_generated\\CyberShake_75','new_generated\\CyberShake_100']
# wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
#             "Montage_25", "Montage_50", "Montage_75", "Montage_100",
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]


[HeftVsCloudHeft(wf_name, reliability) for wf_name in wf_names]





