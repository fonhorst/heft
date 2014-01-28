import random
from environment.ResourceManager import Scheduler, ScheduleItem, Schedule
from reschedulingheft.HeftHelper import HeftHelper


class SimpleRandomizedHeuristic(Scheduler):

     def __init__(self,
                     workflow,
                     nodes,
                     estimator):
            self.workflow = workflow
            self.nodes = nodes
            self.workflow_size = workflow.get_task_count()
            self.estimator = estimator

            self.task_map = {task.id: task for task in HeftHelper.get_all_tasks(self.workflow)}
            self.node_map = {node.name: node for node in nodes}

            self.initial_chromosome = None##GAFunctions.schedule_to_chromosome(initial_schedule)
            pass

     def schedule(self):
         ready_tasks = [child.id for child in self.workflow.head_task.children]
         schedule_mapping = {node: [] for node in self.nodes}

         ##chrmo_mapping = {task_id: self.node_map[node_name] for (task_id, node_name) in chromo}
         task_to_node = dict()
         estimate = self.estimator.estimate_transfer_time

         finished_tasks = set()

         def is_child_ready(child):
            ids = set([p.id for p in child.parents])
            result = False in [id in finished_tasks for id in ids]
            return not result


         def find_slots(node, comm_ready, runtime):
             node_schedule = schedule_mapping.get(node, list())
             free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
             ## TODO: refactor it later
             f_time = max(free_time, comm_ready)
             base_variant = [(f_time, f_time + runtime + 1)]
             zero_interval = [] if len(node_schedule) == 0 else [(0, node_schedule[0].start_time)]
             middle_intervals = [(node_schedule[i].end_time, node_schedule[i + 1].start_time) for i in range(len(node_schedule) - 1)]
             intervals = zero_interval + middle_intervals + base_variant

             result = [(st, end) for (st, end) in intervals if st >= comm_ready and end - st >= runtime]
             return result

         def comm_ready_func(task, node):
                ##TODO: remake this stub later.
                if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
                    return 0
                return max([task_to_node[p.id][2]+ estimate(node, task_to_node[p.id][0], task, p) for p in task.parents])



         def get_possible_execution_times(task, node):
            ## pay attention to the last element in the resulted seq
            ## it represents all available time of node after it completes all its work
            ## (if such interval can exist)
            ## time_slots = [(st1, end1),(st2, end2,...,(st_last, st_last + runtime)]
            runtime = self.estimator.estimate_runtime(task, node)
            comm_ready = comm_ready_func(task, node)
            time_slots = find_slots(node, comm_ready, runtime)
            return time_slots, runtime

         while len(ready_tasks) > 0:
            choosed_index = random.randint(0, len(ready_tasks) - 1)
            task = self.task_map[ready_tasks[choosed_index]]

            choosed_node_index = random.randint(0, len(self.nodes) - 1)
            node = self.nodes[choosed_node_index]

            time_slots, runtime = get_possible_execution_times(task, node)
            choosed_time_index = 0 if len(time_slots) == 1 else random.randint(0, len(time_slots) - 1)
            time_slot = time_slots[choosed_time_index]

            start_time = time_slot[0]
            end_time = start_time + runtime

            item = ScheduleItem(task, start_time, end_time)
            ##schedule_mapping[node].append(item)
            Schedule.insert_item(schedule_mapping, node, item)
            task_to_node[task.id] = (node, start_time, end_time)

            ##print('I am here')
            ready_tasks.remove(task.id)
            finished_tasks.add(task.id)

            ready_children = [child for child in task.children if is_child_ready(child)]
            for child in ready_children:
                ready_tasks.append(child.id)

         schedule = Schedule(schedule_mapping)
         return schedule









