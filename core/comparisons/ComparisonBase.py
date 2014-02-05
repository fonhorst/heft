from datetime import datetime

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
def run(run_name, mainFunc, wf_name, reliability):
    n = 100
    result = [mainFunc(reliability, True, wf_name) for i in range(n)]
    mx_time = max(result)
    min_time = min(result)
    avr_time = sum(result)/n
    ## TODO: fix dispersion calculation
    #avr_dispersion = math.sqrt(sum([math.pow(abs(res - avr_time), 2) for res in result]))
    return (mx_time, min_time, avr_time)

def get_dict(result):
    res = dict()
    res['Max'] = result[0]
    res['Min'] = result[1]
    res['Avr'] = result[2]
    return res

#TODO: fix it later
common_time = datetime.now().strftime("%d_%m_%y %H_%M_%S")
save_path = '..\\..\\resources\\saved_simulation_results\\' + 'HeftVsCloudHeft_' + common_time + '.json'
path_for_gnuplot = '..\\..\\resources\\saved_simulation_results\\' + 'HeftVsCloudHeft_' + common_time + '.txt'
##================Run Heft than CloudHeft

