import os
from matplotlib.patches import Rectangle
from experiments.cga.utilities.common import load_data, generate_pathes
import matplotlib.pyplot as plt

# points = [0, 1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 100, 200, 299]
points = [0, 1, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 100, 120, 140, 150, 170, 190, 200, 220, 240, 250, 270, 290, 299, 350, 400, 450, 499]

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
    best_values = filter(lambda x: len(x[1]) == len(species), best_values)
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


    gens = sorted(data["iterations"], key=lambda x: x["gen"])
    values = [(points.index(gen["gen"]), -1*fit) for gen in gens if gen["gen"] in points
             for fit in gen["solsstat"][0]["fitnesses"]]


    plt.plot([x[0] for x in values], [x[1] for x in values], 'gx')
    pass




def visualize(data, path_to_save=None):
    plt.figure(figsize=(10, 10))


    ## create diversity plot for species
    sp = plt.subplot(3, 1, 1)
    _draw_best_solution_evolution(data)


    ## create solutions diversity plot
    sp = plt.subplot(3, 1, 2)
    _draw_species_diversity(data)

    ## create best solution evolution
    sp = plt.subplot(3, 1, 3)
    _draw_solutions_diversity(data)

    if path_to_save is None:
        plt.show()
    else:
        plt.savefig(path_to_save, dpi=96.0, format="png")
        plt.clf()
    pass





if __name__ == "__main__":
    # path = "../../../temp/vis_test.json"

    # path = "../../../temp/cga_exp/"
    path = "../../../temp/cga_exp_200_50_tr_2/"
    # path = "../../../temp/cga_fixed_mapping/"
    # path = "../../../temp/cga_exp_200interact_50popsize_transfer_10/"
    # path = "../../../temp/cga_exp_with_roulette/"
    # path = "../../../temp/cga_partial_experiments/"



    pathes = generate_pathes(path)

    #names = ["d53fc071-ad6a-409f-9809-db948d1cd1b6.json"]

    for n in pathes:
        visualize(load_data(n), n + ".png")