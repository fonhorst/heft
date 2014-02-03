from functools import partial
import json
from environment.Utility import Utility
from core.comparisons.ComparisonBase import run, get_dict, path_for_gnuplot, save_path
from core.examples.CloudHeftExecutorExample import CloudHeftExecutorExample
from core.examples.HeftExecutorExample import HeftExecutorExample
from core.examples.GAExecutorExample import GAExecutorExample
## Single fire
#main()

mainCloudHeft = CloudHeftExecutorExample().main
mainHeft = HeftExecutorExample().main
mainGA = GAExecutorExample().main

def GAvsHeftGAvsHeftReXGA(wf_name, reliability):

    dax2 = '..\\..\\resources\\' + wf_name + '.xml'
    ## dedicated resource are the same for all bundles
    path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
    bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2))

    mainCloudHEFTwithGA = partial(mainCloudHeft, with_ga_initial=True, the_bundle=bundle)
    mainHEFTwithGA = partial(mainHeft, with_ga_initial=True, the_bundle=bundle)
    mainGAwithBundle = partial(mainGA, the_bundle=bundle)

    print("Calculating now - " + wf_name)
    resGA = run("GA", mainGAwithBundle, wf_name, reliability)
    resHeft = run("Heft + GA", mainHEFTwithGA, wf_name, reliability)
    resCloudHeft = run("HeftREx + GA", mainCloudHEFTwithGA, wf_name, reliability)
    print("===========================")
    pc_hg = (1 - resHeft[2]/resGA[2])*100
    pc_chg = (1 - resCloudHeft[2]/resGA[2])*100
    pc_chh = (1 - resCloudHeft[2]/resHeft[2])*100
    print("          heft + ga vs ga: " + str(pc_hg))
    print("ga + heftREx vs heft + ga: " + str(pc_chh))
    print("       ga + heftREx vs ga: " + str(pc_chg))
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

reliability = 0.95
print("reliability %s" % reliability)
wf_names = ['CyberShake_30']
##TODO: validity check fails here. This wf have something special in its structure. Need to look deeper.
#wf_names = ['Inspiral_72']

[GAvsHeftGAvsHeftReXGA(wf_name, reliability) for wf_name in wf_names]





