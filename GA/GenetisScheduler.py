from GA.GA import GeneticFunctions
import random
class SchedluerGeneticFunctions(GeneticFunctions):
    def __init__(self, target_func,
                     nodes,
                     wf_dag,
                     limit=200, size=400,
                     prob_crossover=0.9,
                     prob_mutation=0.2):
            self.target = target_func
            self.counter = 0
            self.nodes = nodes
            self.wf_dag = wf_dag

            self.limit = limit
            self.size = size
            self.prob_crossover = prob_crossover
            self.prob_mutation = prob_mutation
            pass

    # GeneticFunctions interface impls
    def probability_crossover(self):
        return self.prob_crossover

    def probability_mutation(self):
        return self.prob_mutation

    def initial(self):
        return [self.random_chromo() for j in range(self.size)]

    def fitness(self, chromo):
        ## {Task:Node},{Task:Node},... - fixed length
        ##TODO: remake it later
        return 0

    def check_stop(self, fits_populations):
        self.counter += 1
        if self.counter % 10 == 0:
            best_match = list(sorted(fits_populations))[-1][1]
            fits = [f for f, ch in fits_populations]
            best = max(fits)
            worst = min(fits)
            ave = sum(fits) / len(fits)
            print(
                "[G %3d] score=(%4d, %4d, %4d): %r" %
                (self.counter, best, ave, worst,
                self.chromo2text(best_match)))
            pass
        return self.counter >= self.limit

    def parents(self, fits_populations):
        while True:
            father = self.tournament(fits_populations)
            mother = self.tournament(fits_populations)
            yield (father, mother)
            pass
        pass

    def crossover(self, parents):
        father, mother = parents
        index1 = random.randint(1, len(self.target) - 2)
        index2 = random.randint(1, len(self.target) - 2)
        if index1 > index2:
            index1, index2 = index2, index1
        child1 = father[:index1] + mother[index1:index2] + father[index2:]
        child2 = mother[:index1] + father[index1:index2] + mother[index2:]
        return (child1, child2)

    def mutation(self, chromosome):
        index = random.randint(0, len(self.target) - 1)
        vary = random.randint(-5, 5)
        mutated = list(chromosome)
        mutated[index] += vary
        return mutated

    # internals
    def tournament(self, fits_populations):
        alicef, alice = self.select_random(fits_populations)
        bobf, bob = self.select_random(fits_populations)
        return alice if alicef > bobf else bob

    def select_random(self, fits_populations):
        return fits_populations[random.randint(0, len(fits_populations)-1)]

    def text2chromo(self, text):
        return [ord(ch) for ch in text]

    def chromo2text(self, chromo):
        return "".join(chr(max(1, min(ch, 255))) for ch in chromo)

    def random_chromo(self):
        return [random.randint(1, 255) for i in range(len(self.target))]
    pass
