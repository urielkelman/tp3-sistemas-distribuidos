from abc import abstractmethod

class MessageSet:

    @abstractmethod
    def __contains__(self, item) -> bool:
        pass

    @abstractmethod
    def add(self, item):
        pass