from abc import abstractmethod
from typing import NoReturn


class MessageSet:

    @abstractmethod
    def __contains__(self, item) -> bool:
        pass

    @abstractmethod
    def add(self, item) -> NoReturn:
        pass
