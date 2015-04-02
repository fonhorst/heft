from copy import deepcopy
from functools import reduce
import operator
from pprint import pprint
from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import tracing


def _comm_ready_func(workflow,
                     estimator,
                     task_to_node,
                     chrmo_mapping,
                     task,
                     node):
        estimate = estimator.estimate_transfer_time
        ##TODO: remake this stub later.
        if len(task.parents) == 1 and workflow.head_task.id == list(task.parents)[0].id:
            return 0

        ## TODO: replace it with commented string below later
        res_list = []
        for p in task.parents:
            c1 = task_to_node[p.id][2]
            c2 = estimate(node.name, chrmo_mapping[p.id].name, task, p)
            res_list.append(c1 + c2)

        return max(res_list)
## chrmo_mapping: task-node mapping
def place_task_to_schedule(workflow,
                           estimator,
                           schedule_mapping,
                           task_to_node,
                           chrmo_mapping,
                           task,
                           node,
                           current_time):

        runtime = estimator.estimate_runtime(task, node)
        comm_ready = _comm_ready_func(workflow,
                                      estimator,
                                      task_to_node,
                                      chrmo_mapping,
                                      task,
                                      node)

        def _check(st, end):
            return (0.00001 < (st - current_time)) \
                       and st >= comm_ready and (0.00001 < (end - st) - runtime)

        node_schedule = schedule_mapping.get(node, list())


        ## TODO: add case for inserting between nothing and first element
        size = len(node_schedule)
        result = None
        i = 0
        if size > 0 and _check(0, node_schedule[0]):
            i = -1
            result = (0, node_schedule[0].start_time)
        else:
            while i < size - 1:
                st = node_schedule[i].end_time
                end = node_schedule[i + 1].start_time
                if _check(st, end):
                    break
                i += 1
            if i < size - 1:
                result = (st, end)
            else:
                free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
                ## TODO: refactor it later
                f_time = max(free_time, comm_ready)
                f_time = max(f_time, current_time)
                result = (f_time, f_time + runtime)
                i = size - 1
                pass
            pass

        previous_elt = i
        st_time = result[0]
        end_time = st_time + runtime
        item = ScheduleItem(task, st_time, end_time)

        node_schedule.insert(previous_elt + 1, item)

        schedule_mapping[node] = node_schedule
        return (st_time, end_time)


class NewScheduleBuilder:

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

        ## add nodes which can be in the resource manager, but empty in chromo and fixed schedule
        ## in such they wouldn't be added to the resulted schedule at all
        ## Note: schedule must contain all nodes from ResourceManager always
        for node in self.nodes:
            if node not in schedule_mapping:
                schedule_mapping[node] = []


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

    def __call__(self, chromo, current_time):

        count_of_tasks = lambda mapping: reduce(operator.add, (len(tasks) for node, tasks in mapping.items()), 0)
        alive_nodes = [node for node in self.nodes if node.state != Node.Down]

        alive_nodes_names = [node.name for node in alive_nodes]
        for node_name, tasks in chromo.items():
            if node_name not in alive_nodes_names and len(tasks) > 0:
                raise ValueError("Chromo is invalid. There is a task assigned to a dead node")
        if count_of_tasks(chromo) + len(self.fixed_schedule_part.get_unfailed_tasks_ids()) != len(self.workflow.get_all_unique_tasks()):

            print("==Chromosome==================================")
            print(chromo)
            print("=fixed_schedule_part===================================")
            print(self.fixed_schedule_part)

            raise Exception("The chromosome not a full. Chromo length: {0}, Fixed part length: {1}, workflow size: {2}".
                            format(count_of_tasks(chromo), len(self.fixed_schedule_part.get_unfailed_tasks_ids()),
                                   len(self.workflow.get_all_unique_tasks())))

        # TODO: add not to schedule
        #if count_of_tasks(chromo) + count_of_tasks(self.fixed_schedule_part.mapping) !=

        (schedule_mapping, finished_tasks, ready_tasks, chrmo_mapping, task_to_node) = self._create_helping_structures(chromo)

        # print("SCHEDULE_MAPPING")
        # print("AlIVE_NODES", alive_nodes)
        # pprint(schedule_mapping)

        #chromo_copy = {nd_name: [item for item in items] for (nd_name, items) in chromo.items()}
        chromo_copy = deepcopy(chromo)


        if len(alive_nodes) == 0:
            raise Exception("There are not alive nodes")



        #print("Building started...")
        while len(ready_tasks) > 0:

            # ## TODO: only for debug. Remove it later.
            # print("alive nodes: {0}".format(alive_nodes))
            # for node_name, tasks in chromo_copy.items():
            #     print("Node: {0}, tasks count: {1}".format(node_name, len(tasks)))

            count_before = count_of_tasks(chromo_copy)
            if len(alive_nodes) == 0:
                raise ValueError("Count of alive_nodes is zero")
            for node in alive_nodes:
                if len(chromo_copy[node.name]) == 0:
                    continue
                ## TODO: Urgent! completely rethink this procedure

                tsk_id = None
                for i in range(len(chromo_copy[node.name])):
                    if chromo_copy[node.name][i] in ready_tasks:
                        tsk_id = chromo_copy[node.name][i]
                        break


                if tsk_id is not None:
                    task = self.task_map[tsk_id]
                    #del chromo_copy[node.name][0]
                    chromo_copy[node.name].remove(tsk_id)
                    ready_tasks.remove(tsk_id)

                    (start_time, end_time) = place_task_to_schedule(self.workflow,
                                                                    self.estimator,
                                                                    schedule_mapping,
                                                                    task_to_node,
                                                                    chrmo_mapping,
                                                                    task,
                                                                    node,
                                                                    current_time)

                    task_to_node[task.id] = (node, start_time, end_time)

                    finished_tasks.add(task.id)

                    ready_children = self._get_ready_tasks(task.children, finished_tasks)
                    for child in ready_children:
                        ready_tasks.append(child.id)
            count_after = count_of_tasks(chromo_copy)
            if count_before == count_after:
                raise Exception("Unable to properly process a chromosome."
                                " Perhaps, due to invalid fixed_schedule_part or the chromosome.")
            pass
        schedule = Schedule(schedule_mapping)
        return schedule

    ##TODO: redesign all these functions later

    def _get_ready_tasks(self, children, finished_tasks):
        def _is_child_ready(child):
            for p in child.parents:
                if p.id not in finished_tasks:
                    return False
            return True
        ready_children = [child for child in children if _is_child_ready(child)]
        return ready_children

    pass

