from lstore.table import Table, Record
from lstore.index import Index


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.completed = []
        self.ran = False
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        self.table = table
        # use grades_table for aborting
        self.table = table
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # if self.ran is True:
        #     print("Duplicate transaction")
        #     return True
        # self.ran = True
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result is False:
                print('aborting')
                return self.abort()
            else: # Keep track of the data from completed queries
                self.completed.append((query.__name__, result))
        return self.commit()

    # Rolls back if a query failed
    def abort(self):
        # Iterate over each completed query and undo it
        for type, data in self.completed:
            self.table.abort(type, data)
        return False

    def commit(self):
        # Commit each query to the database
        for type, data in self.completed:
            self.table.commit(type, data)
        # If an update fails, go through each page and delete the entry corresponding to the returned rid
        
        #After Abort is done unlock everything
        self.table.unlock() 
        return False

