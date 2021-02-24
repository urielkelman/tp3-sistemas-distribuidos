from typing import Optional, NoReturn, Any, Callable
from tp2_utils.interfaces.state_commiter import StateCommiter

class DummyStateCommiter(StateCommiter):
    def __init__(self, func: Callable):
        """
        """
        super().__init__()
        self.func = func
        return

    def prepare(self, data: Any) -> Any:
        """
        Applies the function to the data
        :param data: the data
        """
        return self.func(data)

    def flush(self) -> NoReturn:
        """
        Flushes all the state
        """
        return

    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """
        return -1

    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """
        return