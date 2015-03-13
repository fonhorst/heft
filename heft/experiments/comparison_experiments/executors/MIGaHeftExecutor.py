from heft.experiments.comparison_experiments.executors.InheritedGaHeftExecutor import InheritedGaHeftExecutor


class MIGaHeftExecutor(InheritedGaHeftExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.schedule_to_chromosome_converter = kwargs["schedule_to_chromosome_converter"]
        pass

    def _actual_ga_run(self):
        ## TODO: correct this.
        heft_initial = self.schedule_to_chromosome_converter(self.back_cmp.current_schedule)
        heft_initial = self._clean_chromosome(heft_initial, self.back_cmp.event, self.back_cmp.fixed_schedule)

        cleaned_schedule = self.back_cmp.fixed_schedule
        initial_pop = [self._clean_chromosome(ind, self.back_cmp.event, cleaned_schedule) for ind in self.past_pop]

        result = self.ga_builder()(self.back_cmp.fixed_schedule,
                                   heft_initial,
                                   self.current_time,
                                   initial_population=initial_pop)
        ((best_op, pop_op, schedule_op, stopped_iteration_op), logbook_op) = result

        self.past_pop = pop_op

        self._stop_ga()
        return result

    pass