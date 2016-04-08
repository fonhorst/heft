from heft.core.environment.EventMachine import NodeFailed, NodeUp
from heft.experiments.comparison_experiments.executors.GaHeftExecutor import GaHeftExecutor


class InheritedGaHeftExecutor(GaHeftExecutor):
    #@trace
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.past_pop = None
        ## TODO: move to constructor of FailOnce
        self.failed_once = False

        self.estimator = kwargs["estimator"]
        self.chromosome_cleaner = kwargs["chromosome_cleaner"]
        self.replace_anyway = True

        pass

    def init(self):
        result = super().init()
        self.past_pop = result[0][1]
        pass

    def _node_failed_handler(self, event):
        self.current_event = event
        super()._node_failed_handler(event)
        pass

    def _node_up_handler(self, event):
        self.current_event = event
        super()._node_up_handler(event)
        pass

    def _actual_ga_run(self):

        cleaned_schedule = self.back_cmp.fixed_schedule

        initial_pop = [self._clean_chromosome(ind, self.current_event, cleaned_schedule) for ind in self.past_pop]

        result = self.ga_builder()(self.back_cmp.fixed_schedule,
                                   self.back_cmp.initial_schedule,
                                   self.current_time,
                                   initial_population=initial_pop)

        ((best_op, pop_op, schedule_op, stopped_iteration_op), logbook_op) = result

        self.past_pop = pop_op

        self._stop_ga()
        return result

    ## TODO: merge with GAOldPopExecutor
    def _clean_chromosome(self, chromosome, event, current_cleaned_schedule):
        #print("Current event {0}".format(event))
        if isinstance(event, NodeFailed):
            return self.chromosome_cleaner(chromosome, current_cleaned_schedule)
        elif isinstance(event, NodeUp):
             return self.chromosome_cleaner(chromosome, current_cleaned_schedule)
            #return chromosome
        else:
            raise Exception("Unhandled event: {0}".format(event))
        pass

    pass
