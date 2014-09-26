from heft.experiments.comparison_experiments.gaheft_series.aggregators.aggregator import ALG_COLORS


def plot_aggregate_profit_results(alg_colors=ALG_COLORS):

    data = {
        "pso_150": {
            "Montage_25": 240.0,
            "Montage_40": 0.0,
            "Montage_50": 0.0,
            "Montage_75": 0.0,

        },

        "mipso_150": {
            "Montage_25": 220.0,
            "Montage_40": 0.0,
            "Montage_50": 0.0,
            "Montage_75": 0.0,
        }
    }

    # aggr = lambda results: numpy.mean(results)

    def get_points_format(data):
        if len(data) == 0:
            raise ValueError("data is empty")

        item = None
        for alg_name, itm in data.items():
            if wf_name in itm:
                item = itm
                break

        if item is None:
            raise ValueError("data don't contain iformation to plot according to wfs_colors limitations")

        points = []
        for value, results in item[wf_name]["reliability"].items():
            points.append((value, aggr(results)))
        points = sorted(points, key=lambda x: x[0])
        return points

    format_points = get_points_format(data) if reliability is None else [(str(r), 0) for r in reliability]

    plt.grid(True)
    ax = plt.gca()
    # + 1 for legend box
    ax.set_xlim(0, len(format_points) + 1)
    ax.set_xscale('linear')
    plt.xticks(range(0, len(format_points)))
    ax.set_xticklabels([p[0] for p in format_points])
    ax.set_title(wf_name, size=45)
    ax.set_ylabel("profit, %", size=45)
    ax.set_xlabel("reliability", size=45)

    plt.tick_params(axis='both', which='major', labelsize=32)
    plt.tick_params(axis='both', which='minor', labelsize=32)



    if base_alg_name is None:
        raise ValueError("base_alg_name cannot be None")

    d = {alg_name: { wf_name: {} } for alg_name in data}
    for alg_name, item in data.items():
        # wf_name = wf_name.split("_")[0]
        if alg_name not in alg_colors:
            continue

        for value, results in item[wf_name]["reliability"].items():
            if value in reliability or reliability is None:
                d[alg_name][wf_name][value] = aggr(results)
                # points.append((value, aggr(results)))

    for alg_name, item in d.items():
        if alg_name not in alg_colors:
            continue
        points = []
        style = alg_colors[alg_name]
        if alg_name == base_alg_name:
            continue
        for value, res in item[wf_name].items():
            points.append((value, (1 - res/d[base_alg_name][wf_name][value])*100))

        points = sorted(points, key=lambda x: x[0])
        #plt.setp(plt.xticks()[1], rotation=30, ha='right')
        plt.plot([i for i in range(0, len(points))], [x[1] for x in points], style, label=alg_name, linewidth=4.0, markersize=10)
        ax.legend()
    pass

