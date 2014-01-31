from environment.Utility import Utility


def calculate(wf_name, package_name):
    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000
    dax2 = '..\\..\\resources\\' + wf_name + '.xml'
    wf = Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1)
    return wf.avr_runtime(package_name)

def main():
    wf_names = ['CyberShake_30', 'CyberShake_50', 'CyberShake_100']
    package_name = 'SeismogramSynthesis'

    common = [calculate(wn, package_name)for wn in wf_names]
    print(" Avr time execution: " + str(sum(common)/len(common)))

main()