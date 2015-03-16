from functools import partial

from heft.core.environment.ResourceManager import Scheduler, ScheduleItem, Schedule


class PeftHelper(Scheduler):

    @staticmethod
    def peft_rank(wf, rm, estimator):
        nodes = rm.get_nodes()
        ranking = PeftHelper.build_ranking_func(nodes,
                                            lambda job, agent: estimator.estimate_runtime(job, agent),
                                            lambda ni, nj, A, B: estimator.estimate_transfer_time(A, B, ni, nj))
        sorted_tasks = [t.id for t in ranking(wf)]
        return sorted_tasks


    @staticmethod
    def to_nodes(resources):
            result = set()
            for resource in resources:
                result.update(resource.nodes)
            return result

    @staticmethod
    def build_ranking_func(nodes, compcost, commcost, oct):
        task_rank_cache = dict()

        def ranking_func(wf):
            wf_dag = PeftHelper.convert_to_parent_children_map(wf)
            rank = partial(PeftHelper.ranking, nodes=nodes, oct=oct)
            jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)

            ## TODO: sometimes sort gives different results
            ## TODO: it's normal because of only elements with the same rank change their place
            ## TODO: relatively each other with the same rank
            ## TODO: need to get deeper understanding of this situation
            #jbs = [(job, rank(job))for job in jobs]
            #jbs = sorted(jbs, key=lambda x: x[1])
            #jbs = list(reversed(jbs))
            #print("===========JBS=================")
            #for job, rk in jbs:
            #    print("J: " + str(job) + " " + str(rk))
            #print("===========END_JBS=================")

            jobs = sorted(jobs, key=rank)


            return list(reversed(jobs))

        return ranking_func

    @staticmethod
    def ranking(ni, nodes, oct):

        result = sum(oct[(ni,p)] for p in nodes) / len(nodes)
        result = int(round(result, 6) * 1000000) + PeftHelper.get_seq_number(ni)
        return result

    @staticmethod
    def get_seq_number(task):
        ## It is assumed that task.id have only one format ID000[2 digits number]_000
        id = task.id
        number = id[5:7]
        return int(number)


    @staticmethod
    def avr_compcost(ni, nodes, compcost):
        """ Average computation cost """
        return sum(compcost(ni, node) for node in nodes) / len(nodes)

    @staticmethod
    def avr_commcost(ni, nj, pk, pw, nodes, commcost):
        if (pk == pw):
            return 0
        return sum(commcost(ni, nj, pk, a) for a in nodes) / len(nodes)

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

    @staticmethod
    def get_all_tasks(wf):
        map = PeftHelper.convert_to_parent_children_map(wf)
        tasks = [task for task in map.keys()]
        return tasks

    @staticmethod
    def clean_unfinished(schedule):
        def clean(items):
            return [item for item in items if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.EXECUTING]
        new_mapping = {node: clean(items) for (node, items) in schedule.mapping.items()}
        return Schedule(new_mapping)

    @staticmethod
    def get_tasks_for_planning(wf, schedule):
        ## TODO: remove duplicate code later
        def clean(items):
            return [item.job for item in items if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.EXECUTING]
        def get_not_for_planning_tasks(schedule):
            result = set()
            for (node, items) in schedule.mapping.items():
                unfin = clean(items)
                result.update(unfin)
            return result
        all_tasks = PeftHelper.get_all_tasks(wf)
        not_for_planning = get_not_for_planning_tasks(schedule)
        # def check_in_not_for_planning(tsk):
        #     for t in not_for_planning:
        #         if t.id == tsk.id:
        #             return True
        #     return False
        # for_planning = [tsk for tsk in all_tasks if not(check_in_not_for_planning(tsk))]
        for_planning = set(all_tasks) - set(not_for_planning)
        return for_planning


    @staticmethod
    def get_OCT(wf, rm, estimator):
        wf_dag = PeftHelper.convert_to_parent_children_map(wf)
        w = lambda job, agent: estimator.estimate_runtime(job, agent)
        c = lambda ni, nj, A, B: estimator.estimate_transfer_time(A, B, ni, nj)
        task_rank_cache = dict()
        return PeftHelper.create_OCT(wf, rm.get_nodes(), wf_dag, w, c, task_rank_cache)

    @staticmethod
    def create_OCT(wf, nodes, succ, compcost, commcost, task_rank_cache):
        #result = (max(min(getCost(tj, pw) + w(tj,pw) + c(ti,tj)) for pw in nodes) for tj in succ[ti])
        w = compcost
        c = partial(PeftHelper.avr_commcost, nodes=nodes, commcost=commcost)
        jobs = set(succ.keys()) | set(x for xx in succ.values() for x in xx)

        def get_oct_elem(ti, pk):
            result = task_rank_cache.get((ti,pk), None)
            if result is not None:
                return result
            if ti in succ and succ[ti]:

                #result = max(min(get_oct_elem(tj, pw) + w(tj,pw) + c(ti,tj, pk, pw) for pw in nodes) for tj in succ[ti])
                result = max(min(get_oct_elem(tj, pw) + (w(tj,pw))*100 + c(ti,tj, pw, pk) for pw in nodes) for tj in succ[ti])
            else:
                result = 0
            task_rank_cache[(ti,pk)] = result
            return result

        oct = dict()
        for ti in jobs:
            for pk in nodes:
                oct[(ti,pk)] = get_oct_elem(ti, pk)
        return oct




