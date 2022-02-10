from lstore.table import Table, Record
from lstore.index import Index
from threading import Thread

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.worker = None
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        # Create a new thread to run the transactions
        self.worker = Thread(target = self.__run)
        # Start the thread
        self.worker.start()
        pass

    """
    Waits for the worker to finish
    """
    def join(self):
        # Joins the thread with the main thread to wait for completion
        self.worker.join()
        # Nulls self.worker
        self.worker = None
        pass

    # Runs each transaction
    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

