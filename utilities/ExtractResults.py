def aggregate_fails(file_name):
    f = open(file_name, 'r')
    line = f.readline()

    heft_clustering = dict()
    ga_clustering = dict()
    clustering = None
    current_counter = 0
    current_run_result = None

    while line != '':
        if "=== HEFT Run" in line:
            clustering = heft_clustering
            pass
        if "=== GAHEFT Run" in line:
            clustering = ga_clustering
            pass
        if "Run heft" in line:
            current_counter = 0
            pass
        if "Run gaHeft" in line:
            current_counter = 0
            pass
        if "Event: NodeFailed" in line:
            current_counter += 1
            pass
        if "====RESULT====" in line:
            current_run_result = float(f.readline().strip())
            m = clustering.get(current_counter, [])
            m.append(current_run_result)
            clustering[current_counter] = m
            pass
        line = f.readline()
        pass
    f.close()
    ## TODO: add file_params later
    return (file_name, None,heft_clustering, ga_clustering)
    pass
