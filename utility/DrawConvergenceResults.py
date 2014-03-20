import json
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt

# points = [10, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450, 500]
points = [10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 85, 100, 150, 200, 250, 300, 350, 400, 450, 500]

### preprocess data
def _converge_aggr(data):
    # convergence aggregation
    interesting_iter_number = points
    interest_dict = {iter_num: [] for iter_num in interesting_iter_number}
    for el in data:
        #iteration massive
        for record in el[2]:
            iter_num = record["iter"] + 1
            if iter_num in interest_dict.keys():
                interest_dict[iter_num].append(record)
                pass
            pass
        pass

    aggr_dict = dict()
    for inum, items in interest_dict.items():
        if len(items) > 0:
            avr = [i["avr"] for i in items]
            avr_avr = sum(avr)/len(items)
            min_avr = min(avr)
            max_avr = max(avr)

            best = [i["best"] for i in items]
            best_avr = sum(best)/len(items)
            min_best = min(best)
            max_best = max(best)

            aggr_dict[inum] = {"avr_avr": avr_avr, "min_avr": min_avr, "max_avr": max_avr,
                               "best_avr": best_avr, "min_best": min_best, "max_best": max_best,
                               "avr_items": avr, "best_items": best}
        pass
    return aggr_dict

def built_converged_data():

    # load data
    base_path = "../results/"
    filename = "GaRescheduleResults_by_5.json"
    path = base_path + filename

    with open(path, 'r') as f:
        data = json.load(f)

    # convert data to more comfortable representation
    transformed_data = {nm: dict() for nm in set(el["wf_name"] for el in data)}
    for el in data:
        tpls = transformed_data[el["wf_name"]].get(el["task_id"],  ([], []))
        tpls[0].append((el["with_old_pop"]["iter"], el["with_old_pop"]["makespan"], el["with_old_pop"]["pop_aggr"]))
        tpls[1].append((el["with_random"]["iter"], el["with_random"]["makespan"], el["with_random"]["pop_aggr"]))
        transformed_data[el["wf_name"]][el["task_id"]] = tpls

    #extract needed data
    convergence_results = dict()
    for wf_name, tasks in transformed_data.items():
        convergence_results[wf_name] = dict()
        for task_id, (old_pop, random) in tasks.items():
            convergence_results[wf_name][task_id] = (_converge_aggr(old_pop), _converge_aggr(random))
            pass
        pass
    return convergence_results


### visualize

def _sort_dict(dict):
    return sorted(dict.items(), key=lambda x: x[0])

def _draw_variation(type, task_id, old_pop_results, random_results):
    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title(str(task_id))

    tp = type + "_items"

    # num to seqnum
    nts = {points[i]: i for i in range(len(points))}

    oldpop = [(nts[inum], item) for inum, record in _sort_dict(old_pop_results) if int(inum) in points for item in record[tp]]
    rndom = [(nts[inum], item) for inum, record in _sort_dict(random_results) if int(inum) in points for item in record[tp]]

    plt.plot([x[0] for x in oldpop], [x[1] for x in oldpop], 'rx')
    plt.plot([x[0] + 0.2 for x in rndom], [x[1] for x in rndom], 'gx')
    pass

def _draw_task(type, task_id, old_pop_results, random_results):
    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title(str(task_id))

    tp = type + "_avr"

    best_avr_oldpop = [record[tp] for inum, record in _sort_dict(old_pop_results) if int(inum) in points]
    best_avr_random = [record[tp] for inum, record in _sort_dict(random_results) if int(inum) in points]
    plt.plot(best_avr_oldpop, '-rx')
    plt.plot(best_avr_random, '-gD')
    pass

def plot_avrs_by_taskid(data, draw_func):
    # test_json = "D:/wspace/heft/results/backup/convergence_results.json.test"
    # result_json = "D:/wspace/heft/results/convergence_results.json"
    # with open(result_json, 'r') as data_json:
    #     data = json.load(data_json)


    i = 1
    for wf_name, tasks in data.items():
        l = len(tasks) - 1
        for task_id, (old_pop_results, random_results) in _sort_dict(tasks):
            if task_id is not "":

                ## best:
                plt.subplot(l, 2, i)
                draw_func("best", task_id, old_pop_results, random_results)
                i += 1

                ## avr:
                plt.subplot(l, 2, i)
                draw_func("avr", task_id, old_pop_results, random_results)
                i += 1
            pass
        pass


    h1 = Rectangle((0, 0), 1, 1, fc="r")
    h2 = Rectangle((0, 0), 1, 1, fc="g")

    plt.suptitle('Average of Best vs Average of Avr', fontsize=20)
    plt.figlegend([h1, h2], ['with old pop', 'random'], loc='lower center', ncol=10, labelspacing=0. )
    plt.subplots_adjust(hspace=0.5)
    # plt.savefig("D:/wspace/heft/results/convergence_results.png", dpi=128.0, format="png")
    plt.show()
    pass

def plot_variation_by_taskid():
    pass

if __name__ == "__main__":
    data = built_converged_data()
    # plot_avrs_by_taskid(data, _draw_task)
    plot_avrs_by_taskid(data, _draw_variation)



