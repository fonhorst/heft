import distance
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt

from heft.experiments.cga.utilities.common import load_data, generate_pathes


# points = [0, 1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 100, 200, 299]
# points = [0, 1, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 100, 120, 140, 150, 170, 190, 200, 220, 240, 250, 270, 290, 299, 350, 400, 450, 499]
# points = [0, 1, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 100, 120, 140, 150, 170, 190, 200, 220, 240, 250, 270, 290, 299, 350, 400, 450, 499, 600, 700, 800, 900, 999]
points = [0, 1, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 100, 120, 140, 150, 170, 190, 200, 220, 240, 250, 270, 290, 299, 350, 400, 450, 499, 600, 700, 800, 900, 999, 1500, 1999, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]


def _draw_best_solution_evolution(data):
    ## TODO: remake it with acquaring figure and subplot from the function arguments
    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title("Evolution of best")
    ax.set_ylabel("makespan")
    plt.setp(plt.xticks()[1], rotation=30, ha='right')


    gens = sorted(data["iterations"], key=lambda x: x["gen"])
    bests = [(points.index(gen["gen"]), -1*gen["solsstat"][0]["best"]) for gen in gens if gen["gen"] in points]

    plt.plot([x[0] for x in bests], [x[1] for x in bests], '-gD')
    pass

def _draw_species_diversity(data):
    ## TODO: remake it with acquaring figure and subplot from the function arguments
    colors = ['r', 'g', 'b']

    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title("Diversity of species")
    ax.set_ylabel("normalized distance")
    plt.setp(plt.xticks()[1], rotation=30, ha='right')


    species = sorted(data["metainfo"]["species"])
    if len(species) > len(colors):
        raise ValueError("Count of species greater than count available colors.")

    gens = sorted(data["iterations"], key=lambda x: x["gen"])


    values = {s: [(points.index(gen["gen"]), v) for gen in gens if gen["gen"] in points
                  for v in gen["popsstat"][0][s].get("hamming_distances", [0.0])] for s in species}


    pcolors = {s: c for s, c in zip(species, colors)}

    shift = 0.0
    plotted = []
    labels = []
    for s, vals in values.items():
        plt.plot([x[0] + shift for x in vals], [x[1] for x in vals], "{0}x".format(pcolors[s]))

        shift += 0.2
        plotted.append(Rectangle((0, 0), 1, 1, fc=pcolors[s]))
        labels.append(s)

    plt.legend(plotted, labels)


    best_values = [(points.index(gen["gen"]), gen["solsstat"][0].get("best_components", {})) for gen in gens if gen["gen"] in points]
    best_values = list(filter(lambda x: len(x[1]) == len(species), best_values))
    best_values = {s: sorted([(n, bvals[s]) for n, bvals in best_values]) for s in species}

    shift = 0.0
    for s, vals in best_values.items():
        plt.plot([x[0] + shift for x in vals], [x[1] for x in vals], "-mD")
        shift += 0.2

    pass

def _draw_solutions_diversity(data):
    ## TODO: remake it with acquaring figure and subplot from the function arguments
    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title("Diversity of solutions")
    ax.set_ylabel("makespan")
    plt.setp(plt.xticks()[1], rotation=30, ha='right')


    gens = sorted(data["iterations"], key=lambda x: x["gen"])
    values = [(points.index(gen["gen"]), -1*fit) for gen in gens if gen["gen"] in points
             for fit in gen["solsstat"][0]["fitnesses"]]


    plt.plot([x[0] for x in values], [x[1] for x in values], 'gx')
    pass

def _draw_popsstat_component(data, name, title, ylabel, default_value=-1):
     ## TODO: remake it with acquaring figure and subplot from the function arguments
    colors = ['r', 'g', 'b']

    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    plt.setp(plt.xticks()[1], rotation=30, ha='right')

    species = sorted(data["metainfo"]["species"])
    pcolors = {s: c for s, c in zip(species, colors)}

    gens = sorted(data["iterations"], key=lambda x: x["gen"])
    values = {s: [(points.index(gen["gen"]), gen["popsstat"][0][s].get(name, default_value))
                  for gen in gens if gen["gen"] in points]
              for s in species}

    plotted = []
    labels = []
    for s, vals in values.items():
        plt.plot([x[0] for x in vals], [x[1] for x in vals], "-{0}x".format(pcolors[s]))
        plotted.append(Rectangle((0, 0), 1, 1, fc=pcolors[s]))
        labels.append(s)

    plt.legend(plotted, labels)
    pass

def _draw_uniques_inds_count(data):
    _draw_popsstat_component(data, "unique_inds_count",
                             "Count of uniques individuals in populations", "count")


def _draw_diff_between_bests(data):
    ## TODO: remake it with acquaring figure and subplot from the function arguments
    colors = ['r', 'g', 'b']

    plt.grid(True)
    ax = plt.gca()
    ax.set_xlim(0, len(points))
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title("Difference between best individuals in pops")
    ax.set_ylabel("distance")
    plt.setp(plt.xticks()[1], rotation=30, ha='right')

    species = sorted(data["metainfo"]["species"])
    pcolors = {s: c for s, c in zip(species, colors)}

    gens = sorted(data["iterations"], key=lambda x: x["gen"])

    best_values = [(points.index(gen["gen"]), gen["solsstat"][0].get("best_components_itself", {})) for gen in gens if gen["gen"] in points]
    best_values = list(filter(lambda x: len(x[1]) == len(species), best_values))

    diffs = {s: [] for s in species}
    if len(best_values) > 0:
        for s in species:
            previous = best_values[0][1][s]
            for n, bvals in best_values:
                d = distance.hamming(bvals[s], previous, normalized=True)
                previous = bvals[s]
                diffs[s].append((n, d))

    plotted = []
    labels = []
    for s, vals in diffs.items():
        plt.plot([x[0] for x in vals], [x[1] for x in vals], "-{0}x".format(pcolors[s]))
        plotted.append(Rectangle((0, 0), 1, 1, fc=pcolors[s]))
        labels.append(s)

    plt.legend(plotted, labels)
    pass

def _draw_pcm(data):
    _draw_popsstat_component(data, "pcm", "Phenotype convergence measure", "measure")
    pass

def _draw_gdm(data):
    _draw_popsstat_component(data, "gdm", "Genotype diversity measure", "measure")
    pass


def visualize(data, functions, path_to_save=None):
    plt.figure(figsize=(10, 10))

    for i in range(len(functions)):
        plt.subplot(len(functions), 1, i + 1)
        functions[i](data)

    plt.tight_layout()

    if path_to_save is None:
        plt.show()
    else:
        plt.savefig(path_to_save, dpi=96.0, format="png")
        plt.clf()
    pass


def vis_full(data, path_to_save):
    functions = [_draw_best_solution_evolution,
                 _draw_solutions_diversity,
                 _draw_uniques_inds_count,
                 _draw_diff_between_bests,
                 _draw_pcm,
                 _draw_gdm]
    visualize(data, functions, path_to_save)
    pass

def vis_only_best(data, path_to_save):
    functions = [_draw_best_solution_evolution]
    visualize(data, functions, path_to_save)
    pass






if __name__ == "__main__":
    # path = "../../../temp/vis_test.json"

    # path = "../../../temp/cga_exp/"
    path = "../../../temp/dcga_exp/"
    # path = "../../../temp/cga_exp_2/"
    # path = "D:/FTP/cga_exp_archivedmutation"
    # path = "../../../temp/cga_fixed_ordering/"
    # path = "../../../temp/cga_fixed_mapping/"
    # path = "../../../temp/cga_heft_mixin/"
    # path = "../../../temp/cga_improving_mutation/"
    # path = "D:/FTP/cga_exp_1000_50pop_20_10/"
    # path = "../../../temp/cga_exp_200_50_torn2_transf10_ideal20/"
    # path = "../../../temp/cga_fixed_mapping/"
    # path = "../../../temp/cga_exp_200interact_50popsize_transfer_10/"
    # path = "../../../temp/cga_exp_with_roulette/"
    # path = "../../../temp/cga_partial_experiments/"

    vis = vis_only_best

    pathes = generate_pathes(path)

    #names = ["d53fc071-ad6a-409f-9809-db948d1cd1b6.json"]

    for n in pathes:
        vis(load_data(n), n + ".png")