#!/usr/bin/env python3

import os
import ast

from typing import Dict

from tp2_utils.leader_election.bully_connection import BullyConnection


def parse_config_params() -> Dict:
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

    return {"config": bully_connection_config, "host_id": int(os.environ["HOST_ID"])}


LOWEST_LISTENING_PORT = 8000

if __name__ == "__main__":
    config_params = parse_config_params()
    bully_leader_election_connection = BullyConnection(config_params["config"], LOWEST_LISTENING_PORT,
                                                       config_params["host_id"])
    bully_leader_election_connection.start()
