from functools import partial
from environment.ResourceManager import Scheduler


class HeftHelper(Scheduler):

    @staticmethod
    def to_nodes(resources):
            result = set()
            for resource in resources:
                result.update(resource.nodes)
            return result

    @staticmethod
    def build_ranking_func(nodes, compcost, commcost):
        task_rank_cache = dict()

        def ranking_func(wf):
            wf_dag = HeftHelper.convert_to_parent_children_map(wf)
            rank = partial(HeftHelper.ranking, nodes=nodes, succ=wf_dag,
                                     compcost=compcost, commcost=commcost,
                                     task_rank_cache=task_rank_cache)
            jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
            jobs = sorted(jobs, key=rank)
            return list(reversed(jobs))

        return ranking_func

    @staticmethod
    def ranking(ni, nodes, succ, compcost, commcost, task_rank_cache):

        w = partial(HeftHelper.avr_compcost, compcost=compcost, nodes=nodes)
        c = partial(HeftHelper.avr_commcost, nodes=nodes, commcost=commcost)
        ##cnt = partial(self.node_count_by_soft, nodes=nodes)

        def estimate(ni):
            result = task_rank_cache.get(ni,None)
            if result is not None:
                return result
            if ni in succ and succ[ni]:
                ##the last component cnt(ni)/nodes.len is needed to account
                ## software restrictions of particular task
                ## and
                ##TODO: include the last component later
                result = w(ni) + max(c(ni, nj) + estimate(nj) for nj in succ[ni]) ##+ math.pow((nodes.len - cnt(ni)),2)/nodes.len - include it later.
            else:
                result = w(ni)
            task_rank_cache[ni] = result
            return result

        """print( "%s %s" % (ni, result))"""
        result = estimate(ni)
        return result

    @staticmethod
    def avr_compcost(ni, nodes, compcost):
        """ Average computation cost """
        return sum(compcost(ni, node) for node in nodes) / len(nodes)

    @staticmethod
    def avr_commcost(ni, nj, nodes, commcost):
        ##TODO: remake it later.
        return 10
        """ Average communication cost """
        # n = len(nodes)
        # if n == 1:
        #     return 0
        # npairs = n * (n - 1)
        # return 1. * sum(commcost(ni, nj, a1, a2) for a1 in nodes for a2 in nodes
        #                 if a1 != a2) / npairs

    @staticmethod
    def convert_to_parent_children_map(wf):
        head = wf.head_task
        map = dict()
        def mapp(parents, map):
            for parent in parents:
                st = map.get(parent, set())
                st.update(parent.children)
                map[parent] = st
                mapp(parent.children, map)
        mapp(head.children, map)
        return map

    pass
