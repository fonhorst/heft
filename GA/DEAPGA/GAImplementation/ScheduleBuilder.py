from core.HeftHelper import HeftHelper
from environment.ResourceManager import Schedule, ScheduleItem

class ScheduleBuilder:

    def __init__(self,
                 workflow,
                 resource_manager,
                 estimator,
                 task_map,
                 node_map,
                 # fixed part of schedule. It need to be accounted when new schedule is built, but it's not possible to cahnge something inside it
                 fixed_schedule_part):
        self.workflow = workflow
        self.nodes = HeftHelper.to_nodes(resource_manager.get_resources())
        self.estimator = estimator
        ##TODO: Build it
        self.task_map = task_map
        ##TODO: Build it
        self.node_map = node_map

        self.fixed_schedule_part = fixed_schedule_part
        # construct initial mapping
        # eliminate all already scheduled tasks

        pass


    def _create_helping_structures(self, chromo):
        # copy fixed schedule
        # TODO: make common utility function with SimpleRandomizedHeuristic
        def is_last_version_of_task_executing(item):
            return item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.UNSTARTED

        schedule_mapping = {node: [item for item in items] for (node, items) in self.fixed_schedule_part.mapping.items()}

        finished_tasks = [item.job.id for (node, items) in self.fixed_schedule_part.mapping.items() for item in items if is_last_version_of_task_executing(item)]
        finished_tasks = set([self.workflow.head_task.id] + finished_tasks)

        unfinished = [task for task in self.workflow.get_all_unique_tasks() if not task.id in finished_tasks]

        ready_tasks = [task.id for task in self._get_ready_tasks(unfinished, finished_tasks)]



        chrmo_mapping = {item.job.id: self.node_map[node.name] for (node, items) in self.fixed_schedule_part.mapping.items() for item in items if is_last_version_of_task_executing(item)}

        for (node_name, tasks) in chromo.items():
            for tsk_id in tasks:
                chrmo_mapping[tsk_id] = self.node_map[node_name]

        task_to_node = {item.job.id: (node, item.start_time, item.end_time) for (node, items) in self.fixed_schedule_part.mapping.items() for item in items if is_last_version_of_task_executing(item)}

        return (schedule_mapping, finished_tasks, ready_tasks, chrmo_mapping, task_to_node)


    def __call__(self, chromo):

        (schedule_mapping, finished_tasks, ready_tasks, chrmo_mapping, task_to_node) = self._create_helping_structures(chromo)

        chromo_copy = dict()
        for (nd_name, items) in chromo.items():
            chromo_copy[nd_name] = []
            for item in items:
                chromo_copy[nd_name].append(item)


        while len(ready_tasks) > 0:
            for node in self.nodes:
                if len(chromo_copy[node.name]) == 0:
                    continue

                ## TODO: Urgent! completely rethink this procedure
                tsk_id = None
                for i in range(len(chromo_copy[node.name])):
                    if chromo_copy[node.name][i] in ready_tasks:
                        tsk_id = chromo_copy[node.name][i]

                if tsk_id is not None:
                    task = self.task_map[tsk_id]
                    #del chromo_copy[node.name][0]
                    chromo_copy[node.name].remove(tsk_id)
                    ready_tasks.remove(tsk_id)

                    time_slots, runtime = self._get_possible_execution_times(
                                                schedule_mapping,
                                                task_to_node,
                                                chrmo_mapping,
                                                task,
                                                node)

                    time_slot = time_slots[0]
                    start_time = time_slot[0]
                    end_time = start_time + runtime

                    item = ScheduleItem(task, start_time, end_time)

                    Schedule.insert_item(schedule_mapping, node, item)
                    task_to_node[task.id] = (node, start_time, end_time)

                    finished_tasks.add(task.id)

                    #ready_children = [child for child in task.children if self._is_child_ready(finished_tasks, child)]
                    ready_children = self._get_ready_tasks(task.children, finished_tasks)
                    for child in ready_children:
                        ready_tasks.append(child.id)

        schedule = Schedule(schedule_mapping)
        return schedule

    ##TODO: redesign all these functions later

    def _get_ready_tasks(self, children, finished_tasks):
        def _is_child_ready(child):
            ids = set([p.id for p in child.parents])
            result = False in [id in finished_tasks for id in ids]
            return not result
        ready_children = [child for child in children if _is_child_ready(child)]
        return ready_children

    def _find_slots(self,
                    schedule_mapping,
                    node,
                    comm_ready,
                    runtime):
        node_schedule = schedule_mapping.get(node, list())
        free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
        ## TODO: refactor it later
        f_time = max(free_time, comm_ready)
        base_variant = [(f_time, f_time + runtime)]
        zero_interval = [] if len(node_schedule) == 0 else [(0, node_schedule[0].start_time)]
        middle_intervals = [(node_schedule[i].end_time, node_schedule[i + 1].start_time) for i in range(len(node_schedule) - 1)]
        intervals = zero_interval + middle_intervals + base_variant

        result = [(st, end) for (st, end) in intervals if st >= comm_ready and abs((end - st) - runtime) < 0.01]
        return result

    def _comm_ready_func(self,
                         task_to_node,
                         chrmo_mapping,
                         task,
                         node):
        estimate = self.estimator.estimate_transfer_time
        ##TODO: remake this stub later.
        if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
            return 0

        ## TODO: replace it with commented string below later
        res_list = []
        for p in task.parents:
            try:
                c1 = task_to_node[p.id][2]
            except KeyError:
                k = 0
                raise
            c2 = estimate(node, chrmo_mapping[p.id], task, p)
            res_list.append(c1 + c2)

        return max(res_list)
        ##return max([task_to_node[p.id][2] + estimate(node, chrmo_mapping[p.id], task, p) for p in task.parents])

    def _get_possible_execution_times(self,
                                      schedule_mapping,
                                      task_to_node,
                                      chrmo_mapping,
                                      task,
                                      node):
        ## pay attention to the last element in the resulted seq
        ## it represents all available time of node after it completes all its work
        ## (if such interval can exist)
        ## time_slots = [(st1, end1),(st2, end2,...,(st_last, st_last + runtime)]
        runtime = self.estimator.estimate_runtime(task, node)
        comm_ready = self._comm_ready_func(task_to_node,
                                           chrmo_mapping,
                                           task,
                                           node)
        time_slots = self._find_slots(schedule_mapping,
                                      node,
                                      comm_ready,
                                      runtime)
        return time_slots, runtime
    pass