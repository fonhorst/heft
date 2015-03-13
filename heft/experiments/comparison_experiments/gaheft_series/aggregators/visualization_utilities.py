import numpy
from heft.experiments.aggregate_utilities import interval_statistics


def confidence_aggr(results):
    mean, mn, mx, std, left, right = interval_statistics(results)
    ## TODO: I know this is not comletely right..
    confidence_values = [r for r in results if left <= r <= right]
    return numpy.mean(confidence_values) if len(confidence_values) > 0 else mean
