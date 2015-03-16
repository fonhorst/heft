import random
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.algs.pso.crdpso.particle_operations import ConfigurationParticle
from heft.algs.pso.crdpso.crdpso import velocity_update

def config_generate(flops_sum, max_flops, ideal_flops, nodes):
    config = []
    for _ in range(len(nodes)):
        config.append(random.randint((max_flops - ideal_flops), max_flops))
    rel = flops_sum / sum(config)
    config = [round(val * rel) for val in config]
    config = [max_flops if val > max_flops else val for val in config]
    div = flops_sum - sum(config)
    if div > 0:
        config[config.index(min(config))] += div
    if div < 0:
        config[config.index(max(config))] += div
    configuration = {n: c for n, c in zip(sorted([node.name for node in nodes]), config)}
    return ConfigurationParticle(configuration)

def normalization(particle, flops_sum, max_flops):
        config = [round(v) for v in particle.entity.values()]
        config = [max_flops if val > max_flops else val for val in config]
        config = [1 if val < 1 else val for val in config]
        div = flops_sum - sum(config)
        while div != 0:
            if div > 0:
                config[random.randint(0, len(config) - 1)] += div
            if div < 0:
                config[random.randint(0, len(config) - 1)] += div
            config = [max_flops if val > max_flops else val for val in config]
            config = [1 if val < 1 else val for val in config]
            div = flops_sum - sum(config)
        if sum(config) != flops_sum:
            print("DANGER!!!")
        norm_entity = {n: c for n, c in zip(particle.entity.keys(), config)}
        return norm_entity

def make_rm(part):
    return ExperimentResourceManager(rg.r(part.get_nodes()))

def configuration_update(w, c1, c2, p, best, max_flops, flops_sum):
    new_velocity = velocity_update(w, c1, c2, p.best, best, p.velocity, p)
    new_entity = normalization((p + new_velocity), flops_sum, max_flops)
    #test = "p = " + str(p.get_nodes()) + "\t" + "pbest = " + str(p.best.get_nodes()) + "\t" + "gbest = " + str(best.get_nodes()) + " -> "
    p.entity = new_entity
    #test += str(p.get_nodes())
    #print(test)
    p.velocity = new_velocity
    pass


