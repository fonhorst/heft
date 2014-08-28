from builtins import reduce
import functools
from distance import hamming


def force_vector_matrix(pop, kbest, G):
    """
    :return: dictionary of p_uid with array of associated influential powers
    """
    def force(p1, p2):
        """
        estimate force between p1 and p2
        p must be a Position
        """
        dist = hamming(p1, p2)
        f_abs = G * (p1.mass * p2.mass) / dist
        force = {item: p * f_abs for item, p in (p1 - p2).items()}
        return force

    pop = sorted(pop, lambda x: x.mass)
    active_elements = pop[0:kbest]
    forces = {p.uid: [force(p, a) for a in active_elements] for p in pop}
    return forces


# def velocity_and_position(p, fvm, position_func):
#     forces = fvm[p.uid]
#     reduce(operators.add, forces)

