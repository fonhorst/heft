from functools import partial
from experiments.comparison_experiments.common.ComparisonBase import VersusFunctor, profit_print, ComparisonUtility, run
from core.environment import Utility
from experiments.comparison_experiments.common.ExecutorRunner import ExecutorsFactory


class CloudHeftVsHeft(VersusFunctor):
    HEFT = "Heft"
    CLOUD_HEFT = "CloudHeft"

    def __init__(self, reliability, n=100):
        self.reliability = reliability
        ##TODO: simplify this
        # self.mainHeft = HeftExecutorRunner().main
        self.mainHeft = ExecutorsFactory.default().run_heft_executor
        # self.mainCloudHeft = CloudHeftExecutorRunner().main
        self.mainCloudHeft = ExecutorsFactory.default().run_cloudheft_executor
        self.n = n

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

    def __init__(self, reliability, n=100):
        self.reliability = reliability
        self.mainHeft = ExecutorsFactory.default().run_heft_executor
        self.mainGA = ExecutorsFactory.default().run_ga_executor
        self.n = n

    #@save_result
    @profit_print
    def __call__(self, wf_name):
        dax2 = '..\\..\\resources\\' + wf_name + '.xml'
        ## dedicated resource are the same for all bundles
        path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_name))


        mainHEFTwithGA = partial(self.mainHeft, with_ga_initial=False, the_bundle=bundle)
        mainGAwithBundle = partial(self.mainGA, the_bundle=bundle)

        resHeft = run(self.GA_HEFT, mainHEFTwithGA, wf_name, self.reliability, self.n)
        resGA = run(self.GA, mainGAwithBundle, wf_name, self.reliability, self.n)

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

    def __init__(self, reliability, n=100):
        self.reliability = reliability
        self.mainCloudHeft = ExecutorsFactory.default().run_cloudheft_executor
        self.mainHeft = ExecutorsFactory.default().run_heft_executor
        self.mainGA = ExecutorsFactory.default().run_ga_executor
        self.n = n

    #@save_result
    @profit_print
    def __call__(self, wf_name):
        dax2 = '..\\..\\resources\\' + wf_name + '.xml'
        ## dedicated resource are the same for all bundles
        path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_name))

        mainCloudHEFTwithGA = partial(self.mainCloudHeft, with_ga_initial=True, the_bundle=bundle)
        mainHEFTwithGA = partial(self.mainHeft, with_ga_initial=True, the_bundle=bundle)
        mainGAwithBundle = partial(self.mainGA, the_bundle=bundle)

        resGA = run("ga", mainGAwithBundle, wf_name, self.reliability, self.n)
        resHeft = run("Heft + ga", mainHEFTwithGA, wf_name, self.reliability, self.n)
        resCloudHeft = run("HeftREx + ga", mainCloudHEFTwithGA, wf_name, self.reliability, self.n)

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

class GaHeftvsHeft(VersusFunctor):

    GA_HEFT = "gaHeft"
    HEFT = "heft"

    def __init__(self, reliability, n=25):
        self.reliability = reliability
        self.mainHeft = ExecutorsFactory.default().run_heft_executor
        self.mainGaHeft = ExecutorsFactory.default().run_gaheft_executor
        self.n = n

    #@save_result
    @profit_print
    def __call__(self, wf_name, output_file=None):
        print("Run counts: " + str(self.n))

        f = open(output_file, 'a')

        f.write("===============\n")
        f.write("=== HEFT Run\n")
        f.write("===============\n")
        resHeft = run(self.HEFT, self.mainHeft, wf_name, self.reliability, f, n=self.n)
        f.write("===============\n")
        f.write("=== GAHEFT Run\n")
        f.write("===============\n")
        resGaHeft = run(self.GA_HEFT, self.mainGaHeft, wf_name, self.reliability, f, n=self.n)

        pc = (1 - resGaHeft[2]/resHeft[2])*100
        f.write("GaHeft vs Heft: " + str(pc) + '\n')
        f.close()

        result = dict()
        result['wf_name'] = wf_name
        result['algorithms'] = {
            self.HEFT: ComparisonUtility.get_dict(resHeft),
            self.GA_HEFT: ComparisonUtility.get_dict(resGaHeft)
        }
        result['profit_by_avr'] = {"GaHeft vs Heft": pc}
        return result

class GaHeftvsHeftWithWfAdding(VersusFunctor):

    GA_HEFT = "gaHeft"
    HEFT = "heft"

    def __init__(self, n=25, time_koeff=0.1):
        self.mainHeft = ExecutorsFactory.default().run_heft_executor
        self.mainGaHeft = ExecutorsFactory.default().run_gaheft_executor
        self.n = n
        self.time_koeff = time_koeff

    #@save_result
    @profit_print
    def __call__(self, wf_name, output_file=None):
        print("Run counts: " + str(self.n))

        f = open(output_file, 'a')

        f.write("===============\n")
        f.write("=== HEFT Run\n")
        f.write("===============\n")
        resHeft = run(self.HEFT, self.mainHeft, wf_name, 1.0, f, n=self.n)
        f.write("===============\n")
        f.write("=== GAHEFT Run\n")
        f.write("===============\n")
        resGaHeft = run(self.GA_HEFT, self.mainGaHeft, wf_name, 1.0, f, n=self.n)

        pc = (1 - resGaHeft[2]/resHeft[2])*100
        f.write("GaHeft vs Heft with WF adding: " + str(pc) + '\n')
        f.close()

        result = dict()
        result['wf_name'] = wf_name
        result['algorithms'] = {
            self.HEFT: ComparisonUtility.get_dict(resHeft),
            self.GA_HEFT: ComparisonUtility.get_dict(resGaHeft)
        }
        result['profit_by_avr'] = {"GaHeft vs Heft": pc}
        return result