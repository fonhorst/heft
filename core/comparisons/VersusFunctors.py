from functools import partial
from core.comparisons.ComparisonBase import VersusFunctor, profit_print, ComparisonUtility, run
from core.examples.CloudHeftExecutorExample import CloudHeftExecutorExample
from core.examples.GAExecutorExample import GAExecutorExample
from core.examples.HeftExecutorExample import HeftExecutorExample
from environment.Utility import Utility


class CloudHeftVsHeft(VersusFunctor):
    HEFT = "Heft"
    CLOUD_HEFT = "CloudHeft"

    def __init__(self, reliability):
        self.reliability = reliability
        self.mainHeft = HeftExecutorExample().main
        self.mainCloudHeft = CloudHeftExecutorExample().main

    @profit_print
    def __call__(self, wf_name):
        resHeft = run(self.HEFT, self.mainHeft, wf_name, self.reliability)
        resCloudHeft = run(self.CLOUD_HEFT, self.mainCloudHeft, wf_name, self.reliability)

        pc = (1 - resCloudHeft[2]/resHeft[2])*100
        result = dict()
        result['wf_name'] = wf_name
        result['algorithms'] = {
            self.HEFT: ComparisonUtility.get_dict(resHeft),
            self.CLOUD_HEFT: ComparisonUtility.get_dict(resCloudHeft)
        }
        result['profit_by_avr'] = {"heft_ReX vs Heft": pc}
        return result

class GAvsHeftGA(VersusFunctor):

    GA_HEFT = "gaHeft"
    GA = "ga"

    def __init__(self, reliability):
        self.reliability = reliability
        self.mainHeft = HeftExecutorExample().main
        self.mainGA = GAExecutorExample().main

    #@save_result
    @profit_print
    def __call__(self, wf_name):
        dax2 = '..\\..\\resources\\' + wf_name + '.xml'
        ## dedicated resource are the same for all bundles
        path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2))

        mainHEFTwithGA = partial(self.mainHeft, with_ga_initial=True, the_bundle=bundle)
        mainGAwithBundle = partial(self.mainGA, the_bundle=bundle)

        resHeft = run(self.GA_HEFT, mainHEFTwithGA, wf_name, self.reliability)
        resGA = run(self.GA, mainGAwithBundle, wf_name, self.reliability)

        pc = (1 - resHeft[2]/resGA[2])*100

        result = dict()
        result['wf_name'] = wf_name
        result['algorithms'] = {
            self.GA_HEFT: ComparisonUtility.get_dict(resHeft),
            self.GA: ComparisonUtility.get_dict(resGA)
        }
        result['profit_by_avr'] = {"ga_Heft vs ga": pc}
        return result

class GAvsHeftGAvsHeftReXGA(VersusFunctor):

    GA_HEFT = "gaHeft"
    GA = "ga"
    HEFT_REX_GA = "HeftReXGA"

    def __init__(self, reliability):
        self.reliability = reliability
        self.mainCloudHeft = CloudHeftExecutorExample().main
        self.mainHeft = HeftExecutorExample().main
        self.mainGA = GAExecutorExample().main

    #@save_result
    @profit_print
    def __call__(self, wf_name):
        dax2 = '..\\..\\resources\\' + wf_name + '.xml'
        ## dedicated resource are the same for all bundles
        path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2))

        mainCloudHEFTwithGA = partial(self.mainCloudHeft, with_ga_initial=True, the_bundle=bundle)
        mainHEFTwithGA = partial(self.mainHeft, with_ga_initial=True, the_bundle=bundle)
        mainGAwithBundle = partial(self.mainGA, the_bundle=bundle)

        resGA = run("GA", mainGAwithBundle, wf_name, self.reliability)
        resHeft = run("Heft + GA", mainHEFTwithGA, wf_name, self.reliability)
        resCloudHeft = run("HeftREx + GA", mainCloudHEFTwithGA, wf_name, self.reliability)

        pc_hg = (1 - resHeft[2]/resGA[2])*100
        pc_chg = (1 - resCloudHeft[2]/resGA[2])*100
        pc_chh = (1 - resCloudHeft[2]/resHeft[2])*100

        result = dict()
        result['wf_name'] = wf_name
        result['algorithms'] = {
            self.HEFT_REX_GA: ComparisonUtility.get_dict(resCloudHeft),
            self.GA_HEFT: ComparisonUtility.get_dict(resHeft),
            self.GA: ComparisonUtility.get_dict(resGA)
        }
        result['profit_by_avr'] = {
            "ga_heft vs ga": pc_hg,
            "ga_heft_ReX vs ga": pc_chh,
            "ga_heft_ReX vs ga_heft": pc_chg
        }
        return result

