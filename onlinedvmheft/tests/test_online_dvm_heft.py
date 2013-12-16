__author__ = 'nikolay'
import unittest
from functools import partial
from heft.util import reverse_dict

from onlinedvmheft.core import Workflow
from onlinedvmheft.core import Factory
from onlinedvmheft.core import (reschedule, commcost, compcost, Resource, Down, Schedule, ANY_SOFT)

from unittest import TestCase

dag = {3: (5,),
           4: (6,),
           5: (7,),
           6: (7,),
           7: (8, 9)}


class OnlineHEFT(TestCase):






    def test_schedule(self):

        factory = Factory()
        wf = factory.createWf()

        wfs = [wf]

        vm0 = Resource("vm0", Down, 1000)
        vm1 = Resource("vm1", Down, 1000)
        vm2 = Resource("vm2", Down, 1000)

        vm0.soft_types = [ANY_SOFT]
        vm1.soft_types = [ANY_SOFT]
        vm2.soft_types = [ANY_SOFT]


        resources = [vm0, vm1, vm2]

        orders = {resource: [] for resource in resources}
        current_schedule = Schedule("s1", orders)
        time = 0
        up_time = 20
        down_time = 10

        schedule = reschedule(wfs, resources, compcost, commcost, current_schedule, time, up_time, down_time)



        """print(schedule.plan)"""
        printer = lambda list: '\n'.join(["\t" + l.__str__() for l in list])
        print('\n'.join(['%s\t%s' % (k, printer(v)) for k, v in schedule.plan.items()]))


        """
        orders, jobson = schedule(dag, 'abc', compcost, commcost)
        a = jobson[4]
        b = jobson[3]
        c = (set('abc') - set((a, b))).pop()
        print(a, b, c)
        print(orders)
        assert orders == {a: [Event(4, 0, 4), Event(6, 4, 10),
                              Event(7, 11, 18), Event(9, 18, 27)],
                          b: [Event(3, 0, 3), Event(5, 3, 8), Event(8, 21, 29)],
                          c: []}
        """




if __name__ == '__main__':
    unittest.main()
