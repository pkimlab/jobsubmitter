import os.path as op
from typing import Dict, NamedTuple, Optional, Union

import yaml

DATA_DIR = op.join(op.dirname(op.abspath(__file__)), 'data')
DEFAULT_CLUSTERS_FILE = op.join(DATA_DIR, 'clusters.yml')


class ClusterOpts(NamedTuple):
    """Options that are specific to each cluster.

    Notes:
        If `remote_home` and `remote_scratch` are both `None`,
        it implies that they are the same as $HOME on local host.

    Attributes:
        connection_string: SQL-Alchemy style URL of the remote cluster.
        remote_home: Location (or environment variable) corresponding to the HOME folder
            on the remote filesystem.
        remote_scratch: Location (or environment variable) corresponding to the HOME folder
            on the remote filesystem.
        concurrent_job_limit: Maxumum numbe of jobs that can be submitted to the cluster
            at the same time.
    """
    connection_string: str
    remote_home: Optional[str]
    remote_scratch: Optional[str]
    concurrent_job_limit: Optional[int]

    @classmethod
    def _from_file(cls, cluster: str, filename: str = DEFAULT_CLUSTERS_FILE) -> 'ClusterOpts':
        data: Dict[str, Union[str, int]]
        with open(filename, 'r') as fin:
            data = yaml.load(fin)[cluster]  # type: ignore
        return cls._make(data.get(f, None) for f in cls._fields)  # type: ignore
