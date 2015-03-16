def mapping_as_vector(mapping):
    """
    mapping MUST be a dictionary
    converts position to a single vector by joining mapping and ordering structure
    all workflow tasks is sorted by task.id and thus it provides idempotentity for multiple runs
    """
    mapp_string = [node_name for task_id, node_name in sorted(mapping.items(), key=lambda x: x[0])]
    return mapp_string


def cannot_be_zero(number, replace_for_zero=0.0001):
    return replace_for_zero if round(abs(number), 6) < replace_for_zero else number


def gather_info(logbook, stats, g, pop, best, need_to_print=True):
    data = stats.compile(pop) if stats is not None else None
    #data['best'] = best[1].values[0]
    if logbook is not None:
        logbook.record(gen=g, evals=len(pop), **data)
        if need_to_print:
            print(logbook.stream)
    return data

def logbooks_in_data(logbooks, need_print=False):
    res = dict()
    for logbook in logbooks:
        for it in logbook:
            if (it['gen'], 'avg') in res:
                res[(it['gen'], 'avg')] += it['avg']
                res[(it['gen'], 'min')] += it['min']
                #res[(it['gen'], 'best')] += it['best']
            else:
                res[(it['gen'], 'avg')] = it['avg']
                res[(it['gen'], 'min')] = it['min']
                #res[(it['gen'], 'best')] = it['best']
    log_len = len(logbooks)
    for it in range(len(logbooks[0])):
        res[(it, 'avg')] /= log_len
        res[(it, 'min')] /= log_len
        #res[(it, 'best')] /= log_len
        if need_print:
            #print(str(res[(it, 'avg')]) + "\t" + str(res[(it, 'min')]) + "\t" + str(res[(it, 'best')]))
            print(str(res[(it, 'avg')]) + "\t" + str(res[(it, 'min')]))
    return res

def data_to_file(file_path, gen, data, comment=None):
    file = open(file_path, 'w')
    if comment is not None:
        file.write("#" + comment + "\n")
    #file.write("#gen\tavg\tmin\tbest\n")
    file.write("#gen\tavg\tmin\n")
    for i in range(gen):
        #file.write(str(i) + "\t" + str(data[(i, 'avg')]) + "\t" + str(data[(i, 'min')]) + "\t" + str(data[(i, 'best')]) + "\n")
        file.write(str(i) + "\t" + str(data[(i, 'avg')]) + "\t" + str(data[(i, 'min')]) + "\n")
    file.close()

def unzip_result(tuple_list):
    fst_list = [fst for fst, snd in tuple_list]
    snd_list = [snd for fst, snd in tuple_list]
    return fst_list, snd_list






