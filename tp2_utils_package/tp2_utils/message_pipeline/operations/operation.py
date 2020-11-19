from abc import abstractmethod
from typing import Dict, List


class Operation:
    @abstractmethod
    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """

    @classmethod
    def factory(cls, name: str, *args, **kwargs) -> 'Operation':
        """
        Factory pattern for Operation

        :param name: the name of the type to instantiate
        :return: a Operation object
        """
        types = {cls.__name__:cls for cls in Operation.__subclasses__()}
        return types[name](*args, **kwargs)
