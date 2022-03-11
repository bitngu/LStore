from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.completed = 0
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, table, query, *args):
        self.queries.append((query, args))
        # use grades_table for aborting
        self.table = table

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(result)
            else:
                # Keep track of how many queries have ran successfully
                self.completed += 1
        return self.commit(result)

    # Rolls back if a query failed
    def abort(self):
        # Iterate over each completed query and undo it
        for i in range(0, self.completed):
            self.queries[i].abort()
        return False

    def commit(self):
        # Commit each query to the database
        for query in self.queries:
            query.commit()
        return True

