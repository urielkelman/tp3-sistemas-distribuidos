from abc import abstractmethod
from typing import NoReturn, Any, Optional
from tp2_utils.interfaces.state_commiter import StateCommiter


class MessageSet(StateCommiter):

    @abstractmethod
    def __contains__(self, item) -> bool:
        pass

    @abstractmethod
    def prepare(self, data: Any) -> NoReturn:
        """
        Prepares the data to be commited
        :param data: the data to be commited
        """

    @abstractmethod
    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """

    @abstractmethod
    def flush(self) -> NoReturn:
        """
        Flushes the state
        """
        pass

    @abstractmethod
    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """
