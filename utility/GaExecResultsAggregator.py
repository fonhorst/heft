import json
import math

# base_path = "../results_example/"
# filename = "GaRescheduleResults_example.json"
base_path = "../results/"
filename = "GaRescheduleResults_by_5.json"
path = base_path + filename

with open(path, 'r') as f:
    data = json.load(f)

# logbook.record(worst=worst, best=best, avr=avr)
## it is assumed that Data element looks like:
# stat_data = {
#     "wf_name": self.wf_name,
#     "event_name": event.__class__.__name__,
#     "task_id": task_id,
#     "with_old_pop": {
#         "iter": iter_old_pop,
#         "makespan": makespan_old_pop,
#         "pop_aggr": logbook_old_pop
#     },
#     "with_random": {
#         "iter": iter_random,
#         "makespan": makespan_random,
#         "pop_aggr": logbook_random
#     }
# }

## return (avr, sigma)


def aggr(data):
    # iteration aggregations
    iter_avr = sum(el[0] for el in data)/len(data)
    iter_sigma = math.sqrt(sum(math.pow((el[0] - iter_avr), 2) for el in data)/len(data))
    # makespan aggreagtions
    makespan_avr = sum(el[1] for el in data)/len(data)
    makespan_sigma = math.sqrt(sum(math.pow((el[1] - makespan_avr), 2) for el in data)/len(data))



    return (iter_avr, iter_sigma, makespan_avr, makespan_sigma)



transformed_data = {nm: dict() for nm in set(el["wf_name"] for el in data)}

for el in data:
    tpls = transformed_data[el["wf_name"]].get(el["task_id"],  ([], []))
    tpls[0].append((el["with_old_pop"]["iter"], el["with_old_pop"]["makespan"], el["with_old_pop"]["pop_aggr"]))
    tpls[1].append((el["with_random"]["iter"], el["with_random"]["makespan"], el["with_random"]["pop_aggr"]))
    transformed_data[el["wf_name"]][el["task_id"]] = tpls

results = dict()
for wf_name, tasks in transformed_data.items():
    results[wf_name] = dict()
    for task_id, (old_pop, random) in tasks.items():
        results[wf_name][task_id] = (aggr(old_pop), aggr(random))

    overall_old_pop = []
    overall_random = []
    for task_id, (old_pop, random) in tasks.items():
        if task_id == "":
            continue
        overall_old_pop += old_pop
        overall_random += random

    results[wf_name]["overall"] = (aggr(overall_old_pop), aggr(overall_random))


for wf_name, tasks in results.items():
    def form_record(item):
        id, (old_pop, random) = item
        old_pop_info = "\t{0:4.2f}\t{1:4.2f}\t{2:4.2f}\t{3:4.2f}\t".format(old_pop[0], old_pop[1], old_pop[2], old_pop[3])
        random_info = "\t{0:4.2f}\t{1:4.2f}\t{2:4.2f}\t{3:4.2f}\t".format(random[0], random[1], random[2], random[3])
        out = "\t{0:11} {1}|{2}".format(id, old_pop_info, random_info)
        return out
    print("==={0}================".format(wf_name))
    print("\t____________old_pop________________|||||||____________random________________")
    print("\ttask_id\titer_avr\titer_sigma\tmakespan_avr\tmakespan_sigma\t|\titer_avr\titer_sigma\tmakespan_avr\tmakespan_sigma")
    for item in tasks.items():
        id = item[0]
        if id == "" or id == "overall":
            continue
        print(form_record(item))
        pass
    print(form_record(("overall", tasks["overall"])))
    pass

###
# visualize here
###
def converge_aggr(data):
    # convergence aggregation
    interesting_iter_number = [10, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450, 500]
    interest_dict = {iter_num: [] for iter_num in interesting_iter_number }
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
        avr = [i["avr"] for i in items]
        avr_avr = sum(avr)/len(items)
        min_avr = min(avr)
        max_avr = max(avr)

        best = [i["best"] for i in items]
        best_avr = sum(best)/len(items)
        min_best = min(best)
        max_best = max(best)

        aggr_dict[inum] = {"avr_avr": avr_avr, "min_avr": min_avr, "max_avr": max_avr,
                           "best_avr": best_avr, "min_best": min_best, "max_best": max_best}
        pass
    return aggr_dict

convergence_results = dict()
for wf_name, tasks in transformed_data.items():
    convergence_results[wf_name] = dict()
    for task_id, (old_pop, random) in tasks.items():
        convergence_results[wf_name][task_id] = (converge_aggr(old_pop), converge_aggr(random))
        pass
    pass

import matplotlib.pyplot as plt
plt.plot([1,2,3,4])
plt.ylabel('some numbers')
plt.show()





