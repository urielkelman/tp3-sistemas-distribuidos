from typing import Dict, NoReturn, Any
from abc import abstractmethod

class GroupAggregate:
    @abstractmethod
    def add(self, key: str, values: Dict) -> NoReturn:
        """
        Adds an element to the group statistic

        :param key: the key of the element
        :param values: the values associated
        """
        pass

    @abstractmethod
    def dump(self) -> Dict[Any, Dict]:
        """
        Dumps all the statistics to a dict
        :return: the dict with the group value as key and a dict of statistics
        """

    @classmethod
    def factory(cls, name: str, *args, **kwargs) -> 'GroupAggregate':
        """
        Factory pattern for GroupAggregate

        :param name: the name of the type to instantiate
        :return: a GroupAggregate object
        """
        types = {cls.__name__:cls for cls in GroupAggregate.__subclasses__()}
        return types[name](*args, **kwargs)
