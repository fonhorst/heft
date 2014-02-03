from functools import partial
import json
from environment.Utility import Utility
from core.comparisons.ComparisonBase import run, get_dict, path_for_gnuplot, save_path
from core.examples.HeftExecutorExample import HeftExecutorExample
from core.examples.GAExecutorExample import GAExecutorExample
## Single fire
#main()


mainHeft = HeftExecutorExample().main
mainGA = GAExecutorExample().main

def GaVsHeftGa(wf_name, reliability):

    dax2 = '..\\..\\resources\\' + wf_name + '.xml'
    ## dedicated resource are the same for all bundles
    path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
    bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2))

    mainHEFTwithGA = partial(mainHeft, with_ga_initial=True, the_bundle=bundle)
    mainGAwithBundle = partial(mainGA, the_bundle=bundle)

    print("Calculating now - " + wf_name)
    resHeft = run("Heft + GA", mainHEFTwithGA, wf_name, reliability)
    resGA = run("GA", mainGAwithBundle, wf_name, reliability)
    print("===========================")
    pc = (1 - resHeft[2]/resGA[2])*100
    print("heft + ga vs ga: " + str(pc))
    print("===========================")

    result = dict()
    result['wf_name'] = wf_name
    result['heft'] = get_dict(resHeft)
    result['ga'] = get_dict(resGA)
    result['profit_by_avr'] = pc

    f = open(save_path, 'a')
    json.dump(result, f)
    f.close()

    g = open(path_for_gnuplot, 'a')
    heft = "" + "_heftGa" + " " + str(resHeft[2]) + " " + wf_name + '\n'
    cheft = "" + "___ga" + " " + str(resGA[2]) + " " + wf_name + '\n'
    g.writelines([heft, cheft])
    g.close()


reliability = 0.95
print("reliability %s" % reliability)
wf_names = ["Epigenomics_24"]

[GaVsHeftGa(wf_name, reliability) for wf_name in wf_names]





