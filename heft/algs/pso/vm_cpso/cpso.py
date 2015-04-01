from copy import deepcopy
import random
import math
import numpy
from heft.algs.common.utilities import gather_info
from heft.core.environment.BaseElements import Node
from collections import namedtuple
from deap import tools
from deap.base import Toolbox
from heft.algs.pso.vm_cpso.ordering_operators import build_schedule, generate, ordering_update, fitness
from heft.algs.pso.vm_cpso.mapping_operators import update as mapping_update
from heft.algs.pso.vm_cpso.configuration_particle import config_generate, configuration_update
from heft.algs.pso.vm_cpso.mapordschedule import merge_rms
from heft.algs.pso.vm_cpso.particle_operations import ConfigurationParticle, MappingParticle

Env = namedtuple('Env', ['wf', 'rm', 'estimator'])

class VMCoevolutionPSO():

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

        init_particle = generate(_wf, rm, estimator, schedule=self.kwargs['initial_schedule'], fixed_schedule_part=fix_sched)

        pop_gen = lambda n: ([generate(_wf, rm, estimator, fixed_schedule_part=fix_sched) for _ in range(n - 1)] + [init_particle])

        config_gen = lambda n: ([config_generate(rm) for _ in range(n - 1)] + [deepcopy(ConfigurationParticle(rm))])

        def compound_update(w, c1, c2, p, best, min=-1, max=1):
            mapping_update(w, c1, c2, p.mapping, best.mapping)
            ordering_update(w, c1, c2, p.ordering, best.ordering, min=min, max=max)

        def config_update(w, c1, c2, p, best, init_rm):
            configuration_update(w, c1, c2, p, best, init_rm)

        toolbox = Toolbox()
        toolbox.register("sched_pop_gen", pop_gen)
        toolbox.register("conf_pop_gen", config_gen)
        toolbox.register("fitness", fitness, _wf, estimator, rm, fix_sched, current_time)
        toolbox.register("comp_update", compound_update)
        toolbox.register("config_update", config_update)
        return toolbox

    def __call__(self):

        params = self.kwargs
        invalidate_fitness = True

        w, c1, c2 = params["w"], params["c1"], params["c2"]
        gen_steps = params['generations']

        init_rm = params['env'][1]

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

        best_index = 0
        change_chance = params['hall_idx_change_chance']

        for g in range(gen_steps):
            #print("g: {0}".format(g))

            leaders, winner = self.gamble(sched_pop, conf_pop, n, self.toolbox)

            p1_leader = leaders[random.randint(0, len(leaders)-1)][0][0]
            p2_leader = leaders[random.randint(0, len(leaders)-1)][0][1]

            # fitness for pop1
            for p in sched_pop:
                if not hasattr(p, "fitness") or not p.fitness.valid:
                    particles_adapter(p, p2_leader.entity)
                    p.fitness = self.toolbox.fitness(p, p2_leader)
                if not p.best or p.best.fitness < p.fitness:
                    p.best = deepcopy(p)
                if not best or hall_of_fame[hall_of_fame_size-1][1] < p.fitness:
                    hall_of_fame = self.change_hall(hall_of_fame, ((p, p2_leader), p.fitness), hall_of_fame_size)

            # fitness for pop2
            for p in conf_pop:
                p1_leader_copy = deepcopy(p1_leader)
                if not hasattr(p, "fitness") or not p.fitness.valid:
                    particles_adapter(p1_leader_copy, p.entity)
                    p.fitness = self.toolbox.fitness(p1_leader_copy, p)
                if not p.best or p.best.fitness < p.fitness:
                    p.best = deepcopy(p)
                if not best or hall_of_fame[hall_of_fame_size-1][1] < p.fitness:
                    hall_of_fame = self.change_hall(hall_of_fame, ((p1_leader_copy, p), p.fitness), hall_of_fame_size)

            # is winner best?
            if hall_of_fame[hall_of_fame_size-1][1] < winner[1]:
                hall_of_fame = self.change_hall(hall_of_fame, winner, hall_of_fame_size)

            # Gather all the fitnesses in one list and print the stats
            winner[0][0].fitness = winner[1]
            gather_info(self.logbook, self.stats, g, (sched_pop+conf_pop+[winner[0][0]]), hall_of_fame[0], False)

            # Best update
            best_index = self.change_index(best_index, change_chance, hall_of_fame_size)
            best = hall_of_fame[best_index]

            # update pop1
            for p in sched_pop:
                self.toolbox.comp_update(w, c1, c2, p, best[0][0])
            if invalidate_fitness and not g == gen_steps-1:
                for p in sched_pop:
                    del p.fitness

            # update pop2
            for p in conf_pop:
                self.toolbox.config_update(w, c1, c2, p, best[0][1], init_rm)
            if invalidate_fitness and not g == gen_steps-1:
                for p in conf_pop:
                    del p.fitness

        # Gather result
        hall_of_fame.sort(key=lambda p: p[1], reverse=True)
        self.best = hall_of_fame[0]
        self.pops = [sched_pop, conf_pop]
        self.hall = hall_of_fame
        return self.result()

    def result(self):
        return self.best, self.pops, self.logbook, self.initial_pops, self.hall, self.vm_series


    def change_hall(self, hall, part, size):
        """
        try to add new particle to the hall
        """
        if part[1] in [p[1] for p in hall]:
            return hall
        hall.append(deepcopy(part))
        hall.sort(key=lambda p: p[1], reverse=True)
        return hall[0:size]

    def change_index(self, idx, chance, size):
        """
        try to change index of best from hall_of_fame
        """
        if random.random() < chance:
            rnd = int(random.random() * size)
            while (rnd == idx):
                rnd = int(random.random() * size)
            return rnd
        return idx

    def gamble(self, pop1, pop2, n, toolbox):
        """
        gamble between pop1 and pop2 to make leaders list
        """
        games = {}
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

def particles_adapter(sched, config):
    """
    During init and update process, some assigned nodes in mapping not existed in config.
    Therefore, it is required to change this nodes according to nodes from current config.
    """
    nodes = [node for node in config.get_live_nodes()]
    nodes_names = [node.name for node in nodes]
    new_mapping = dict()
    for task, node in sched.mapping.entity.items():
        if node not in nodes_names:
            new_mapping[task] = nodes[random.randint(0, len(nodes) - 1)].name
        else:
            new_mapping[task] = node
    sched.mapping.entity = new_mapping



def vm_run_cpso(**kwargs):
    """
    welcome to the cpso
    """
    kwargs_copy = deepcopy(kwargs)
    cpso = VMCoevolutionPSO(**kwargs)
    VMCoevolutionPSO.vm_series = []
    res = cpso()
    res_rm = res[0][0][1].entity
    kw_rm = kwargs_copy['env'][1]

    # Change current resource manager
    new_rm = merge_rms(kw_rm, res_rm)
    for node in new_rm.get_all_nodes():
        if node.name not in [r_node.name for r_node in res_rm.get_all_nodes()]:
            node.state = Node.Down

    kwargs['env'][1].resources[0].nodes = new_rm.resources[0].nodes

    return res








