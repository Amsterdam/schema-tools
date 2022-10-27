from pathlib import Path


def group_dataset_args(dataset_args):
    paths = []
    dataset_ids = []
    for dataset_arg in dataset_args:
        if Path(dataset_arg).exists():
            paths.append(dataset_arg)
        else:
            dataset_ids.append(dataset_arg)
    return paths, dataset_ids
