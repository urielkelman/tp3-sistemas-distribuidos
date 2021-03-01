from abc import abstractmethod
from typing import Optional, NoReturn, Any


class StateCommiter:
    def __init__(self, recover_state_on_init: bool = True,
                 commit_number: Optional[int] = None):
        """
        Implements two-phase commits

        :param recover_state_on_init: whether to recover last state or not
        :param commit_number: the commit number from which to restore the state
        """
        return

    @abstractmethod
    def prepare(self, data: Any) -> Any:
        """
        Prepares the data to be commited
        :param data: the data to be commited
        """

    @abstractmethod
    def flush(self) -> NoReturn:
        """
        Flushes all the state
        """

    @abstractmethod
    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """

    @abstractmethod
    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """

    @abstractmethod
    def commit_done_cleanup(self):
        pass
