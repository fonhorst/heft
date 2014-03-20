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




# a_save = open("D:/wspace/heft/results/convergence_results.json", 'w')
# json.dump(convergence_results, a_save)
# a_save.close()






