from abc import abstractmethod


class NodeBehaviour:
    @abstractmethod
    def execute_tasks(self):
        pass
