from copy import deepcopy
import functools
import numpy
import math
import random
from heft.algs.common.utilities import cannot_be_zero, gather_info
from heft.core.environment.BaseElements import Node
from collections import namedtuple
from deap import tools
from deap.base import Toolbox

from heft.algs.gsa.vm_cgsa.operators import G, Kbest, particles_adapter, merge_rms
from heft.algs.gsa.vm_cgsa.ordering_mapping_operators import force, mapping_update, ordering_update, CompoundParticle, generate
from heft.algs.gsa.vm_cgsa.ordering_operators import fitness
from heft.algs.gsa.vm_cgsa.configuration_particle import config_generate, configuration_update
from heft.algs.gsa.vm_cgsa.particle_operations import ConfigurationParticle, MappingParticle
from heft.algs.heft.DSimpleHeft import run_heft

def _randvecsum(vectors):
    l = len(vectors[0])
    val = [_randsum(v[i] for v in vectors) for i in range(l)]
    return val

def _randsum(iterable):
    add = lambda a, b: a + random.random()*b
    return functools.reduce(add, iterable)

Env = namedtuple('Env', ['wf', 'rm', 'estimator'])

class VMCoevolutionGSA():

    def __init__(self, **kwargs):
        self.kwargs = deepcopy(kwargs)
        self.initial_pops = []
        self.vm_series = []

        self.env = kwargs["env"]
        self.fixed_schedule = kwargs["fixed_schedule"]
        self.current_time = kwargs["current_time"]

        self.stats = tools.Statistics(lambda ind: ind.fitness.values[0])
        self.stats.register("avg", numpy.mean)
        self.stats.register("std", numpy.std)
        self.stats.register("min", numpy.min)
        self.stats.register("max", numpy.max)

        self.logbook = tools.Logbook()
        self.logbook.header = ["gen", "evals"] + self.stats.fields + ["best"]
        self.kwargs['logbook'] = self.logbook

        self.toolbox = self.toolbox_init()
        self.kwargs['toolbox'] = self.toolbox

        pass

    def toolbox_init(self):

        _wf, rm, estimator = self.env
        fix_sched = self.fixed_schedule
        current_time = self.current_time
        w, c = self.kwargs['w'], self.kwargs['c']

        init_particle = generate(_wf, rm, estimator, schedule=self.kwargs['initial_schedule'], fixed_schedule_part=fix_sched)

        pop_gen = lambda n: ([generate(_wf, rm, estimator, fixed_schedule_part=fix_sched) for _ in range(n - 1)] + [init_particle])

        config_gen = lambda n: ([config_generate(rm) for _ in range(n - 1)] + [deepcopy(ConfigurationParticle(rm))])

        def compound_update(w, c, p, min=-1, max=1):
            mapping_update(w, c, p.mapping)
            ordering_update(w, c, p.ordering, min=min, max=max)

        def config_update(w, c, init_rm, p):
            configuration_update(w, c, p, init_rm)

        def compound_force(p, pop, kbest, G):
            mapping_force = force(p.mapping, (p.mapping for p in pop), kbest, G)
            ordering_force = force(p.ordering, (p.ordering for p in pop), kbest, G)
            return (mapping_force, ordering_force)

        def configuration_force(p, pop, kbest, G):
            return force(p, pop, kbest, G)

        def heft_solution():
            heft_sol = run_heft(_wf, rm, estimator)
            heft_part = (generate(_wf, rm, estimator, schedule=heft_sol, fixed_schedule_part=fix_sched), deepcopy(ConfigurationParticle(rm)))
            return heft_part

        toolbox = Toolbox()
        toolbox.register("sched_pop_gen", pop_gen)
        toolbox.register("conf_pop_gen", config_gen)
        toolbox.register("fitness", fitness, _wf, estimator, rm, fix_sched, current_time)
        toolbox.register("comp_update", compound_update, w, c)
        toolbox.register("config_update", config_update, w, c, rm)
        toolbox.register("comp_force", compound_force)
        toolbox.register("config_force", configuration_force)
        toolbox.register("G", G)
        toolbox.register("kbest", Kbest)
        toolbox.register("heft_solution", heft_solution)
        return toolbox

    def __call__(self):

        params = self.kwargs

        kbest_init, ginit = params['kbest'], params['ginit']
        G = ginit
        kbest = kbest_init
        gen_steps = params['generations']

        n = params['pop_size']
        sched_pop = params['initial_population']
        conf_pop = None

        if sched_pop is None:
            sched_pop = self.toolbox.sched_pop_gen(n)
            conf_pop = self.toolbox.conf_pop_gen(n)

        best = params.get('best', None)

        hall_of_fame = []
        hall_of_fame_size = params['hall_of_fame_size']
        if hall_of_fame_size == 0:
            raise ValueError("hall_of_fame_size = 0")

        best_idx = 0
        change_chance = params['hall_idx_change_chance']

        for i in range(gen_steps):

            leaders, winner = self.gamble(sched_pop, conf_pop, n, self.toolbox)

            p1_leader = leaders[random.randint(0, len(leaders)-1)][0][0]
            p2_leader = leaders[random.randint(0, len(leaders)-1)][0][1]

            for p in sched_pop:
                particles_adapter(p, p2_leader.entity)
                p.fitness = self.toolbox.fitness(p, p2_leader)
                if len(hall_of_fame) < hall_of_fame_size or hall_of_fame[hall_of_fame_size-1][1] < p.fitness:
                    hall_of_fame = self.change_hall(hall_of_fame, ((p, p2_leader), p.fitness), hall_of_fame_size)

            for p in conf_pop:
                p1_leader_copy = deepcopy(p1_leader)
                particles_adapter(p1_leader_copy, p.entity)
                p.fitness = self.toolbox.fitness(p1_leader_copy, p)
                if len(hall_of_fame) < hall_of_fame_size or hall_of_fame[hall_of_fame_size-1][1] < p.fitness:
                    hall_of_fame = self.change_hall(hall_of_fame, ((p1_leader_copy, p), p.fitness), hall_of_fame_size)

            # is winner best?
            if len(hall_of_fame) < hall_of_fame_size or hall_of_fame[hall_of_fame_size-1][1] < winner[1]:
                hall_of_fame = self.change_hall(hall_of_fame, winner, hall_of_fame_size)

            best_idx = self.change_index(best_idx, change_chance, len(hall_of_fame))
            best = hall_of_fame[best_idx]

            best[0][0].mass = 0.00005
            best[0][1].mass = 0.00005
            #print(str(i) + "\t" + "BEST: fitness = " + str(best[1]) + "\t" + "nodes: " + str(best[0][1].get_nodes()))

            ##statistics gathering
            winner[0][0].fitness = winner[1]
            gather_info(self.logbook, self.stats, i, (sched_pop+conf_pop+[winner[0][0]]), hall_of_fame[0], False)

            ## mass estimation
            ## It is assumed that a minimization task is solved
            sched_pop = sorted(sched_pop, key=lambda x: x.fitness)
            conf_pop = sorted(conf_pop, key=lambda x: x.fitness)
            best_fit = sched_pop[0].fitness
            worst_fit = sched_pop[-1].fitness
            best_fit2 = conf_pop[0].fitness
            worst_fit2 = conf_pop[-1].fitness
            max_diff = best_fit.values[0] - worst_fit.values[0]
            max_diff = cannot_be_zero(max_diff)
            max_diff2 = best_fit2.values[0] - worst_fit2.values[0]
            max_diff2 = cannot_be_zero(max_diff2)

            """
            for p in pop:
                p.mass = cannot_be_zero((best_fit.values[0] - p.fitness.values[0]) / max_diff)
            for p in pop2:
                p.mass = cannot_be_zero((best_fit2.values[0] - p.fitness.values[0]) / max_diff2)
            """
            for p in sched_pop:
                p.mass = cannot_be_zero((p.fitness.values[0] - worst_fit.values[0]) / max_diff)
            for p in conf_pop:
                p.mass = cannot_be_zero((p.fitness.values[0] - worst_fit2.values[0]) / max_diff2)


            ## estimate all related forces
            ## fvm is a matrix of VECTORS(due to the fact we are operating in d-dimensional space) size of 'pop_size x kbest'
            ## in fact we can use wrapper for the entity of pop individual but python has duck typing,
            ## so why don't use it, if you use it carefully?
            for p in sched_pop:
                p.force = self.toolbox.comp_force(p, sched_pop+[best[0][0]], kbest, G)
            for p in conf_pop:
                p.force = self.toolbox.config_force(p, conf_pop+[best[0][1]], kbest, G)



            ## compute new velocity and position
            for p in sched_pop:
                self.toolbox.comp_update(p)
            for p in conf_pop:
                self.toolbox.config_update(p)

            ## change gravitational constants
            G = self.toolbox.G(ginit, i, gen_steps)
            kbest = self.toolbox.kbest(kbest_init, kbest, i, gen_steps)
            #print("G = " + str(G) + "\t" + "kbest = " + str(kbest))

            ##removing temporary elements
            for p in sched_pop:
                if hasattr(p, 'fitness'): del p.fitness
                if hasattr(p, 'acceleration'): del p.acceleration
            for p in conf_pop:
                if hasattr(p, 'fitness'): del p.fitness
                if hasattr(p, 'acceleration'): del p.acceleration
            pass
        hall_of_fame.sort(key=lambda p: p[1], reverse=True)

        self.best = hall_of_fame[0]
        self.pops = [sched_pop, conf_pop]
        self.hall = hall_of_fame

        return self.result()

    def result(self):
        return self.best, self.pops, self.logbook, self.initial_pops, self.hall, self.vm_series

    def gamble(self, pop1, pop2, n, toolbox):
        """
        gamble between pop1 and pop2 to make leaders list
        """
        games = {}

        # add heft solution
        h1, h2 = toolbox.heft_solution()
        games[(h1, h2)] = toolbox.fitness(h1, h2)

        for _ in range(self.kwargs['gamble_size']):
            p1 = deepcopy(pop1[random.randint(0, n - 1)])
            p2 = deepcopy(pop2[random.randint(0, n - 1)])
            particles_adapter(p1, p2.entity)
            games[(p1, p2)] = toolbox.fitness(p1, p2)
        leaders = [(k, v) for k, v in games.items()]
        leaders.sort(key=lambda item: item[1], reverse=True)
        size = self.kwargs['leader_list_size']
        leaders = leaders[:size]
        return leaders, leaders[0]


    def change_hall(self, hall, part, size):
        if part[1] in [p[1] for p in hall]:
            return hall
        hall.append(deepcopy(part))
        hall.sort(key=lambda p: p[1], reverse=True)
        return hall[0:size]

    def change_index(self, idx, chance, size):
        if size == 1:
            return idx
        if random.random() < chance:
            rnd = random.randint(0, size - 1)
            while rnd == idx:
                rnd = random.randint(0, size - 1)
            return rnd
        return idx

def vm_run_cgsa(**kwargs):
    """
    welcome to the cgsa
    """
    kwargs_copy = deepcopy(kwargs)
    cgsa = VMCoevolutionGSA(**kwargs)
    VMCoevolutionGSA.vm_series = []
    res = cgsa()
    res_rm = res[0][0][1].entity
    kw_rm = kwargs_copy['env'][1]

    # Change current resource manager
    new_rm = merge_rms(kw_rm, res_rm)
    for node in new_rm.get_all_nodes():
        if node.name not in [r_node.name for r_node in res_rm.get_all_nodes()]:
            node.state = Node.Down

    kwargs['env'][1].resources[0].nodes = new_rm.resources[0].nodes

    return res

