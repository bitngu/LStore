from lstore.table import Table, Record
from lstore.index import Index
import math

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self.data = None
        self.type = None
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        # Set the type of the query to be undone
        self.type = 'delete'
        rid = self.table.locate_rid(primary_key)
        if not rid:
            return False
        rid = rid[0]
        # Save the rid for abort
        self.data = rid
        ret  = self.table.RID.grab_page(math.floor((rid - 1) / 512)).half_write( 0xFFFFFFFF, (rid - 1) % 512, True, False)
        return True if ret else False
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # Set the type of the query to be undone
        self.type = 'insert'
        # Format the data for insertion by table
        data = columns
        # Call to the table to handle insertion into its pages
        write = self.table.write(data)
        # Save rid
        self.data = write
        # Check that write returned successfully
        if not write:
            return False
        else:
            return True

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        # Performs a table read to get the data
        ret = self.table.read(index_value, index_column, query_columns)
        if not ret:
            return False
        return ret

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # Set the type of the query to be undone
        self.type = 'update'
        # Add the updated information to the table with a table write
        # Perform a table update
        ret = self.table.update(primary_key, columns)
        # Save the data
        self.data = ret
        if not ret:
            return False
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        nums  = self.table.locate_range(start_range, end_range, aggregate_column_index)
        if len(nums) == 0:
            return False
        return sum(nums)

    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def abort(self):
        self.table.abort(self.type, self.data)
        return True
