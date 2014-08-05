def mapping_as_vector(mapping):
    """
    mapping MUST be a dictionary
    converts position to a single vector by joining mapping and ordering structure
    all workflow tasks is sorted by task.id and thus it provides idempotentity for multiple runs
    """
    mapp_string = [node_name for task_id, node_name in sorted(mapping.items(), key=lambda x: x[0])]
    return mapp_string


