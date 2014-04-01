from datetime import datetime
import json
import os
from Tools.Scripts.ndiff import fopen
from filelock.filelock import FileLock


class VersusFunctor:
    def __call__(self, wf_name):
        pass

def profit_print(f):
    def inner_func(*args, **kwargs):
        result = f(*args, **kwargs)
        print("==================================")
        for (key, value) in result['profit_by_avr'].items():
            print("{0}: {1}".format(key, value))
        print("==================================")
        return result
    return inner_func

def output(f):
    def inner_func(*args, **kwargs):
        result = (mx_time, min_time, avr_time) = f(*args, **kwargs)
        print("==============common results: " + args[0] + " " + args[2] + "================")
        print("           Max: " + str(mx_time))
        print("           Min: " + str(min_time))
        print("           Avr: " + str(avr_time))
        return result
    return inner_func

@output
def run(run_name, mainFunc, wf_name, reliability, output_file=None, n=100):

    result = []
    for i in range(n):
        iter_str = "Run " + str(run_name) + " " + str(i) + '\n'
        #print(iter_str)

        if output_file is not None:
            output_file.write(iter_str)

        class wrt:
            def write(self, record):
                output_file.write("\t" + record)
            def flush(self):
                output_file.flush()
        logger = None if output_file is None else wrt()

        res = mainFunc(reliability, True, wf_name, logger=logger)

        if output_file is not None:
            output_file.write("\t====RESULT====\n")
            output_file.write('\t' + str(res) + '\n')
            output_file.flush()

        result.append(res)
        pass
    mx_time = max(result)
    min_time = min(result)
    avr_time = sum(result)/n

    if output_file is not None:
        output_file.write("max: " + str(mx_time) + '\n')
        output_file.write("min: " + str(min_time) + '\n')
        output_file.write("avr: " + str(avr_time) + '\n')
    ## TODO: fix dispersion calculation
    #avr_dispersion = math.sqrt(sum([math.pow(abs(res - avr_time), 2) for res in result]))
    return (mx_time, min_time, avr_time)






class ComparisonUtility:

    SAVE_PATH_TEMPLATE = '..\\..\\resources\\saved_simulation_results\\{0}_{1}.json'

    @staticmethod
    def get_dict(result):
        res = dict()
        res['Max'] = result[0]
        res['Min'] = result[1]
        res['Avr'] = result[2]
        return res

    @staticmethod
    def cur_time():
        common_time = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
        return common_time

    @staticmethod
    def build_save_path(name):
        path = ComparisonUtility.SAVE_PATH_TEMPLATE.format(name, ComparisonUtility.cur_time())
        return path


class ResultSaver:
    def __init__(self, path):
        # path to save result of function
        self.path = path
        self.dir = os.path.dirname(self.path)

    def __call__(self, result):
        result_arrays = []

        if not os.path.exists(self.dir):
            ## TODO: remove it later.
            print("dir to create: " + str(self.dir))
            os.makedirs(self.dir)

        if os.path.exists(self.path):
            with FileLock(self.path):
                a_save = open(self.path, 'r')
                result_arrays = json.load(a_save)
                a_save.close()

        with FileLock(self.path):
            a_save = open(self.path, 'w')
            result_arrays.append(result)
            json.dump(result_arrays, a_save)
            a_save.close()

        print("results saved")
        return result




#save_path = '..\\..\\resources\\saved_simulation_results\\' + 'HeftVsCloudHeft_' + common_time + '.json'
#path_for_gnuplot = '..\\..\\resources\\saved_simulation_results\\' + 'HeftVsCloudHeft_' + common_time + '.txt'
##================Run Heft than CloudHeft

