import random
def mapping_as_vector(mapping):
    """
    mapping MUST be a dictionary
    converts position to a single vector by joining mapping and ordering structure
    all workflow tasks is sorted by task.id and thus it provides idempotentity for multiple runs
    """
    mapp_string = [node_name for task_id, node_name in sorted(mapping.items(), key=lambda x: x[0])]
    return mapp_string

def cannot_be_zero(number, replace_for_zero=0.000001):
    return replace_for_zero if round(abs(number), 6) < 0.000001 else number

def gather_info(logbook, stats, g, pop, best, need_to_print=True):
    """
    for co-evolution scheme, it is required to record best, instead of min of population
    """
    data = stats.compile(pop) if stats is not None else None
    if best is not None:
        data['best'] = best[1].values[0]
    if logbook is not None:
        logbook.record(gen=g, evals=len(pop), **data)
        if need_to_print:
            if best is None:
                print(logbook.stream)
            else:
                print(logbook.stream)#print(logbook.stream + "\t" + str(round(data['best'], 2)))
    return data

def logbooks_in_data(logbooks, with_best=False, need_print=False):
    """
    Reduce several logbooks from experiment to the one logbook with average data.
    Used in cpso cgsa
    """
    res = dict()
    for logbook in logbooks:
        for it in logbook:
            if (it['gen'], 'avg') in res:
                res[(it['gen'], 'avg')] += it['avg']
                res[(it['gen'], 'min')] += it['min']
                if with_best:
                    res[(it['gen'], 'best')] += it['best']
            else:
                res[(it['gen'], 'avg')] = it['avg']
                res[(it['gen'], 'min')] = it['min']
                if with_best:
                    res[(it['gen'], 'best')] = it['best']
    log_len = len(logbooks)
    for it in range(len(logbooks[0])):
        res[(it, 'avg')] /= log_len
        res[(it, 'min')] /= log_len
        if with_best:
            res[(it, 'best')] /= log_len
        if need_print:
            if with_best:
                print(str(res[(it, 'avg')]) + "\t" + str(res[(it, 'min')]) + "\t" + str(res[(it, 'best')]))
            else:
                print(str(res[(it, 'avg')]) + "\t" + str(res[(it, 'min')]))
    return res

def data_to_file(file_path, gen, data, with_best=False, comment=None):
    """
    Write logbook data to the file.
    Used in cpso cgsa.
    """
    file = open(file_path, 'w')
    if comment is not None:
        file.write("#" + comment + "\n")
    if with_best:
        file.write("#gen\tavg\tmin\tbest\n")
    else:
        file.write("#gen\tavg\tmin\n")
    for i in range(gen):
        if with_best:
            file.write(str(i) + "\t" + str(data[(i, 'avg')]) + "\t" + str(data[(i, 'min')]) + "\t" + str(data[(i, 'best')]) + "\n")
        else:
            file.write(str(i) + "\t" + str(data[(i, 'avg')]) + "\t" + str(data[(i, 'min')]) + "\n")
    file.close()

def unzip_result(tuple_list):
    """
    Just an unzip list of tuple to 2 lists
    """
    fst_list = [fst for fst, snd in tuple_list]
    snd_list = [snd for fst, snd in tuple_list]
    return fst_list, snd_list

def weight_random(list):
    """
    return index of choosen element after weight random
    """
    summ = sum(list)
    norm = [v / summ for v in list]
    rnd = random.random()
    idx = 0
    stack = norm[idx]
    while rnd >= stack:
        idx += 1
        stack += norm[idx]
    return idx
