from itertools import zip_longest
import json
import os
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt
import matplotlib

##=========================================
## Settings
##=========================================
import numpy
from scoop import futures

ALL = None
# base_path = "../results/m_[30x3]/m100_[30x3]_10by10_tour4/"
# tasks_to_draw = ALL
tasks_to_draw = ["ID00000_000", "ID00010_000", "ID00020_000", "ID00030_000", "ID00050_000"]

# tasks_to_draw = ["ID00000_000", "ID00005_000", "ID00010_000", "ID00015_000", "ID00020_000", "ID00025_000",
#                  "ID00030_000", "ID00035_000", "ID00040_000", "ID00045_000", "ID00050_000"]
# tasks_to_draw = ["ID00055_000", "ID00060_000", "ID00065_000", "ID00070_000", "ID00075_000", "ID00080_000",
#                  "ID00085_000", "ID00090_000", "ID00095_000", "ID00099_000"]
# tasks_to_draw = ["ID00000_000", "ID00010_000", "ID00020_000", "ID00030_000"]
# tasks_to_draw = ["ID00090_000"]
# tasks_to_draw = ["ID00050_000", "ID00060_000", "ID00070_000", "ID00090_000"]
# tasks_to_draw = ["ID00080_000", "ID00090_000"]

# tasks_to_draw = ["ID000{0}_000".format(str(i) if i > 10 else "0" + str(i)) for i in range(0, 8)]

# points = [10, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450, 500]
# points = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 85, 100, 150, 200, 250, 300, 350, 400, 450, 500]
# points = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 85, 100]
points = [1] + [(i+1)*5 for i in range(19)] + [99]

matplotlib.rc('xtick', labelsize=10)

items_proccessing_lower_threshold = 5

##========================================


### preprocess data
def _converge_aggr(data, only_best=0):
    """
    only_best param can be -1, 0, 1
    if only_best == 0 than use all data
    if only_best == -1 than use only worst data
    if only_best == 1 than use only best data
    """
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

            # best = [i["best"] for i in items]
            # best = [i["best"] for i in items]
            best = [i["best"] for i in items]
            best = sorted(best)
            if isinstance(only_best, tuple) and len(only_best) == 2:
                start, end = only_best
                best = best[start: end]
            elif isinstance(only_best, int):
                best = best[0:only_best] if only_best != 0 else best
            elif isinstance(only_best, str):
                m_best = numpy.mean(best)
                std_best = numpy.std(best)
                best = [b for b in best if (m_best - std_best) <= b <= (m_best + std_best)]

            best_avr = sum(best)/len(best)
            if (inum == 10 or inum == 15 or inum == 20 or inum == 25):
                print("inum {0} value {1}".format(inum, best_avr))
            min_best = min(best)
            max_best = max(best)

            aggr_dict[inum] = {"avr_avr": avr_avr, "min_avr": min_avr, "max_avr": max_avr,
                               "best_avr": best_avr, "min_best": min_best, "max_best": max_best,
                               "avr_items": avr, "best_items": best, "count": len(items)}
        pass
    return aggr_dict

def built_converged_data(base_path):

    # load data

    #filename = "GaRescheduleResults_by_5.json"
    filename = "small_run.json"
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
            if tasks_to_draw == ALL or task_id.strip() in tasks_to_draw:
                print("task_id " + str(task_id))
                print("old")
                op = _converge_aggr(old_pop, only_best="mean")
                print("rand")
                rnd = _converge_aggr(random, only_best="mean")
                convergence_results[wf_name][task_id] = (op, rnd)
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

    # p1 = Rectangle((0, 0), 1, 1, fc="g")
    # p2 = Rectangle((0, 0), 1, 1, fc="r")
    # p3 = Rectangle((0, 0), 1, 1, fc="#00A2E8")
    # plt.legend([p1, p2, p3], ["GA", "IGA", "HEFT"])
    pass

def _draw_pref_profit(type, task_id, old_pop_results, random_results):
    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    # plt.setp(plt.xticks()[1], rotation=5, ha='right')

    ## TODO: remove it later.
    d = {
        "ID00000_000": 0.3298,
        "ID00010_000": 0.3042,
        "ID00020_000": 0.2352,
        "ID00040_000": 0.1851,
        "ID00050_000": 0.1632,
        "ID00070_000": 0.1447,
        "ID00090_000": 0.0740,
    }
    # lb = lambda x: "{:0.2f}".format(x*d[task_id.strip()])
    lb = lambda x: "{0}".format(x)
    points_labels = [lb(p) for p in points]
    # points_labels = [lb(0)] + [lb(p) for p in points if p != 1 or p != 99] + [lb(100)]
    ####################################

    ax.set_xticklabels(points_labels)
    ax.set_title(str(task_id))

    tp = type + "_avr"

    best_avr_oldpop = [record[tp] for inum, record in _sort_dict(old_pop_results) if int(inum) in points]
    best_avr_random = [record[tp] for inum, record in _sort_dict(random_results) if int(inum) in points]

    f = lambda o, r: (1 - o/r)*100 if (o is not None) and (r is not None) else 0
    data = [f(o, r) for (o, r) in zip_longest(best_avr_oldpop, best_avr_random)]
    plt.plot(data, '-bx')
    pass

def plot_avrs_by_taskid(data, draw_func, base_path, filename):
    # test_json = "D:/wspace/heft/results/backup/convergence_results.json.test"
    # result_json = "D:/wspace/heft/results/convergence_results.json"
    # with open(result_json, 'r') as data_json:
    #     data = json.load(data_json)

    plt.figure(figsize=(10, 10))


    i = 1
    for wf_name, tasks in data.items():
        l = len(tasks) - 1 if "" in tasks.keys() else len(tasks)
        for task_id, (old_pop_results, random_results) in _sort_dict(tasks):
            if task_id is not "":

                ## two column representation

                # ## best:
                # plt.subplot(l, 2, i)
                # draw_func("best", task_id, old_pop_results, random_results)
                # i += 1
                #
                # ## avr:
                # plt.subplot(l, 2, i)
                # draw_func("avr", task_id, old_pop_results, random_results)
                # i += 1

                ## best:
                plt.subplot(l, 1, i)
                draw_func("best", task_id, old_pop_results, random_results)
                i += 1
            pass
        pass


    h1 = Rectangle((0, 0), 1, 1, fc="r")
    h2 = Rectangle((0, 0), 1, 1, fc="g")
    h3 = Rectangle((0, 0), 1, 1, fc="b")



    plt.suptitle('Average of Best vs Average of Avr', fontsize=20)
    plt.figlegend([h1, h2, h3], ['with old pop', 'random', 'perf profit'], loc='lower center', ncol=10, labelspacing=0. )
    plt.subplots_adjust(hspace=0.5)
    # plt.tight_layout()
    plt.savefig(base_path + filename, dpi=56.0, format="png")
    plt.clf()
    ##plt.show()
    pass

def plot_variation_by_taskid():
    pass

def visualize(path):
    print("Processing path {0}...".format(path))
    data = built_converged_data(path)
    plot_avrs_by_taskid(data, _draw_task, path, "values.png")
    plot_avrs_by_taskid(data, _draw_variation, path, "variation.png")
    plot_avrs_by_taskid(data, _draw_pref_profit, path, "perf.png")

def visualize2(path):
    print("Processing path {0}...".format(path))
    data = built_converged_data(path)

    inums_oldpop = dict()
    inums_random = dict()
    for wf_name, tasks in data.items():
        for task_id, (old_pop_results, random_results) in _sort_dict(tasks):



            for inum, record in _sort_dict(old_pop_results):
                if int(inum) in points:
                    dt = inums_oldpop.get(inum, [])
                    dt.append(record["best_avr"])
                    inums_oldpop[inum] = dt

            for inum, record in _sort_dict(random_results):
                if int(inum) in points:
                    dt = inums_random.get(inum, [])
                    dt.append(record["best_avr"])
                    inums_random[inum] = dt
            pass



    result_oldpop = {i: sum(values)/len(values) for i, values in inums_oldpop.items()}
    result_random = {i: sum(values)/len(values) for i, values in inums_random.items()}

    result = {i_1: (1 - v_1/v_2)*100 for((i_1, v_1), (i_2, v_2)) in zip_longest(_sort_dict(result_oldpop), _sort_dict(result_random))}

    plt.figure(figsize=(10, 10))
    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title("Overall")
    plt.yticks([i/5 for i in range(0, 100)])

    data = [v for (i, v) in _sort_dict(result)]
    plt.plot(data, '-bx')

    h1 = Rectangle((0, 0), 1, 1, fc="r")
    h2 = Rectangle((0, 0), 1, 1, fc="g")
    h3 = Rectangle((0, 0), 1, 1, fc="b")



    plt.suptitle('Average of Best vs Average of Avr', fontsize=20)
    plt.figlegend([h1, h2, h3], ['with old pop', 'random', 'perf profit'], loc='lower center', ncol=10, labelspacing=0. )
    plt.subplots_adjust(hspace=0.5)
    plt.savefig(path + "overall.png", dpi=128.0, format="png")
    plt.clf()
    pass



if __name__ == "__main__":


    # folder = "D:/wspace/heft/results/good_for_GA_IGA/[Montage_100]_[50]_[10by50]_[19_04_14_16_44_43]/"
    # folder = "C:/Users/nikolay/Documents/the_third_paper/review_improvements/f6-7/1/"
    # folder = "D:/wspace/heft/results/m250/"
    # folder = "D:/wspace/heft/results/[Montage_250]_[50]_[10by20]_[18_06_14_17_52_26]/"
    # folder = "D:/wspace/heft/results/[Montage_250]_[50]_[10by20]_[18_06_14_18_16_37]/"
    # folder = "D:/wspace/heft/results/[Montage_250]_[50]_[10by5]_[18_06_14_18_41_42]/"
    # folder = "D:/wspace/heft/results/m250_[120-180]/"
    # folder = "D:/wspace/heft/results/[Montage_250]_[50]_[10by20]_[18_06_14_19_09_24]/"
    # folder = "D:/wspace/heft/results/[Montage_250]_[50]_[10by5]_[19_06_14_10_43_15]/"
    # folder = "C:/Users/nikolay/Documents/the_third_paper/good/good/mcopy/"
    folder = "D:/wspace/heft/results/[Montage_100]_[50]_[10by1]_[20_06_14_13_13_51]/"
    # folder = "D:/wspace/heft/results/m250_[120-180]_good120/"

    # folder = "D:/wspace/heft/results/m_[20x3]/tournament/m40(35)_[20x3]_5by10_tour4/"
    def generate_pathes(folder):
        pathes = []
        for entry in os.listdir(folder):
            p = folder + entry
            if os.path.isdir(p):
                pathes.extend(generate_pathes(p + "/"))
            elif entry == "small_run.json":
                pathes.append(folder)
        return pathes

    pathes = generate_pathes(folder)

    # pathes = ["D:/wspace/heft/results/m_[50x3]/tournament/m100_[50x3]_10by35_tour4/",
    #             "D:/wspace/heft/results/m_[30x3]/with_tournament/m50_[30x3]_10by60_tour4/",
    #             "D:/wspace/heft/results/m_[30x3]/with_tournament/m40(35)_[30x3]_5by60_tour4/"]

    # pathes = ["D:/wspace/heft/results/"]
    # pathes = ["D:/wspace/heft/results/good_results/m100_gaheft_oldpop_10by10_2/"]

    # list(map(visualize2, pathes))
    # list(futures.map(visualize2, pathes))
    list(futures.map(visualize, pathes))
    # list(map(visualize, pathes))


    #data = built_converged_data()
    #plot_avrs_by_taskid(data, _draw_task, "values.png")
    #plot_avrs_by_taskid(data, _draw_variation, "variation.png")
    #plot_avrs_by_taskid(data, _draw_pref_profit, "perf.png")



