#!/usr/bin/env python3

import os
import ast

from typing import Dict

from tp2_utils.leader_election.bully_connection import BullyConnection


def parse_config_params() -> (Dict, Dict):
    """
    Parse env variables to find program config params
    Function that search and parse program configuration parameters in the
    program environment variables. If at least one of the config parameters
    is not found a KeyError exception is thrown. If a parameter could not
    be parsed, a ValueError is thrown. If parsing succeeded, the function
    returns a map with the env variables.
    """
    hosts_ids = ast.literal_eval(os.environ["HOSTS_IDS"])
    hosts_ips = ast.literal_eval(os.environ["HOSTS_IPS"])
    hosts_ports = ast.literal_eval(os.environ["PORTS"])
    bully_connection_config = {}
    for i in range(len(hosts_ids)):
        bully_connection_config[hosts_ids[i]] = (hosts_ips[i], hosts_ports[i])

    workers_hosts = ast.literal_eval(os.environ["WORKER_HOSTS"])
    workers_images = ast.literal_eval(os.environ["WORKER_IMAGES"])
    workers_volumes = ast.literal_eval(os.environ["WORKER_VOLUMES"])
    workers_env_variables = ast.literal_eval(os.environ["WORKER_ENV_VARIABLES"])
    workers_entrypoints = ast.literal_eval(os.environ["WORKER_ENTRYPOINTS"])

    workers_config = {}
    for i in range(len(workers_hosts)):
        workers_config[workers_hosts[i]] = {
            "image": workers_images[i],
            "volume": {
                workers_volumes[i][0]: {
                    "bind": workers_volumes[i][0]
                }
            },
            "environment": workers_env_variables[i],
            "entrypoint": workers_entrypoints[i]
        }

    return {"config": bully_connection_config, "host_id": int(os.environ["HOST_ID"])}, workers_config


LOWEST_LISTENING_PORT = 8000

if __name__ == "__main__":
    bully_config, workers_config = parse_config_params()

    import logging
    logging.info(workers_config)

    bully_leader_election_connection = BullyConnection(bully_config["config"], workers_config, LOWEST_LISTENING_PORT,
                                                       bully_config["host_id"])
    bully_leader_election_connection.start()
