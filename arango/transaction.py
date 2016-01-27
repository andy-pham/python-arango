
class TransactionStep(object):

    __slots__ = ('collections', 'action')

    def __init__(self, action, collections=None):
        self.action = action
        self.collections = [] if collections is None else collections
