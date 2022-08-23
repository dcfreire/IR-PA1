from multiprocessing.managers import SyncManager, BaseProxy

from .frontier import Frontier


class FrontierManager(SyncManager):
    pass


FrontierManager.register("Frontier", Frontier)
