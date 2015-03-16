import random


def run_sa(toolbox, stats, logbook, initial_solution, T, N):
    """
    Simple Simulated Annealing implementation
    toolbox must contain the following methods:
    energy - value of objective which needs to be optimized
    update_T
    neighbor
    transition_probability
    attempts_count
    """
    ## initialization
    current_solution = initial_solution
    best = current_solution
    current_solution.energy = toolbox.energy(current_solution)
    g = 0
    ## whole run
    while round(T, 4) > 0.0:

        data = stats.compile([current_solution]) if stats is not None else {}
        if logbook is not None:
            logbook.record(gen=g, T=T,  **data)
            print(logbook.stream)

        attempt_count = toolbox.attempts_count(T)
        for _ in range(attempt_count):
            new_sol = toolbox.neighbor(current_solution)
            new_sol.energy = toolbox.energy(new_sol)
            tprob = toolbox.transition_probability(current_solution, new_sol, T)
            if random.random() < tprob:
                current_solution = new_sol
                break
        best = max(best, current_solution, key=lambda x: x.energy)

        T = toolbox.update_T(T, N, g)
        g += 1
        pass

    return best, logbook, current_solution






