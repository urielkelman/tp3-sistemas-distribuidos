from typing import NamedTuple, List, Dict, Callable, Optional, Any

from yaml import Loader
from yaml import load

from tp2_utils.message_pipeline.message_pipeline import MessagePipeline
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.rabbit_utils.publisher_sharding import PublisherSharding
from tp2_utils.message_pipeline.message_set.disk_message_set import DiskMessageSet


class ConsumerProducerServiceConfig(NamedTuple):
    message_pipeline: MessagePipeline
    host: str
    consume_from: str
    produce_to: List[str]
    messages_to_group: int
    publisher_sharding: Optional[Any]


def load_config(config_path: str,
                func_dict: Dict[str, Callable]) -> ConsumerProducerServiceConfig:
    """
    Loads the config for the server

    :param config_path: the path where to load the config
    :return: nothing
    """
    operations = {}
    group_aggregates = {}

    with open(config_path, "r") as yaml_file:
        config_dict = load(yaml_file, Loader=Loader)

    host = config_dict['rabbit_params']['host']
    consume_from = config_dict['rabbit_params']['consume_from']
    produce_to = config_dict['rabbit_params']['produce_to']
    messages_to_group = config_dict['rabbit_params']['messages_to_group']
    publisher_sharding = None
    message_set = None
    if 'message_set_params' in config_dict:
        message_set = DiskMessageSet(**config_dict['message_set_params'])
    if 'publisher_sharding' in config_dict:
        publisher_sharding = PublisherSharding(**config_dict['publisher_sharding'])
    for group_aggregate in config_dict['group_aggregates']:
        op = GroupAggregate.factory(group_aggregate['type'], **group_aggregate['args'])
        group_aggregates[group_aggregate['name']] = op
    for operation in config_dict['operations']:
        for k, v in operation['args'].items():
            if isinstance(v, str) and v in func_dict:
                operation['args'][k] = func_dict[v]
        if operation['type'] == 'GroupBy':
            operation['args']['aggregates'] = [group_aggregates[agg_name] for agg_name in
                                               operation['args']['aggregates']]
        op = Operation.factory(operation['type'], **operation['args'])
        operations[operation['name']] = op
    if 'message_pipeline_kwargs' in config_dict:
        message_pipeline = MessagePipeline([operations[op_name]
                                            for op_name in config_dict['message_pipeline']],
                                           idempotency_set=message_set,
                                           **config_dict['message_pipeline_kwargs'])
    else:
        message_pipeline = MessagePipeline([operations[op_name]
                                            for op_name in config_dict['message_pipeline']],
                                           idempotency_set=message_set)
    return ConsumerProducerServiceConfig(host=host, consume_from=consume_from,
                                         produce_to=produce_to,
                                         messages_to_group=messages_to_group,
                                         message_pipeline=message_pipeline,
                                         publisher_sharding=publisher_sharding)
