"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index:

    def __init__(self, table):
        # One index for each column. All our empty initially.
        self.indices = [None] * table.num_columns
        self.table = table
        self.key = table.key
        # Create indexes for all parameters
        self.create_index(self.key)
        # for i in range(0, table.num_columns):
        #     self.create_index(i)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return self.indices[column][value] if value in self.indices[column] else False

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create empty index on specific column
    """

    def create_index(self, column_number):
        # Validate column_number is in range
        if column_number >= len(self.indices) or column_number < 0:
            return False
        # Check for existing index
        if self.indices[column_number] is not None:
            return self.indices[column_number]
        # Iterate over exiting pages and add all values into the index
        self.indices[column_number] = self.table.getIndexColumn(column_number)
        # Return success
        return True
    
    # Add a new record to the index
    def add_rid(self, rid):
        
        # Check each possible index
        for column_number in range(0, self.table.num_columns):
            # Exit if no index for the column
            if self.indices[column_number] is None:
                continue
            # Get the value
            value = self.table.locate_column_by_rid(rid, column_number)
            # Initialize the value index if necessary
            if not value in self.indices[column_number]:
                self.indices[column_number][value] = [rid]
            else:
                if rid not in self.indices[column_number][value]:
                    # Add the rid
                    self.indices[column_number][value].append(rid)
        return True
    
    # Edit a record in the index
    def update_rid(self, rid):
        # Check each possible index
        for column_number in range(0, self.table.num_columns):
            # Exit if no index for the column
            if self.indices[column_number] is None:
                continue
            # Find and remove the instance of the rid
            for val in self.indices[column_number]:
                if rid in self.indices[column_number][val]:
                    self.indices[column_number][val].remove(rid)
            # Get the value
            value = self.table.locate_column_by_rid(rid, column_number)
            # Initialize the value index if necessary
            if not value in self.indices[column_number]:
                self.indices[column_number][value] = []
            # Add the rid
            self.indices[column_number][value].append(rid)
        return True
    
    # Delete a record in the index
    def delete_rid(self, rid):
        # Check each possible index
        for column_number in range(0, self.table.num_columns):
            # Exit if no index for the column
            if self.indices[column_number] is None:
                continue
            # Find and remove the instance of the rid
            for val in self.indices[column_number]:
                if rid in self.indices[column_number][val]:
                    self.indices[column_number][val].remove(rid)
                    break
        return True

    def isIndex(self, column_number):
        return self.indices[column_number] is not None
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number >= len(self.indices) or column_number < 0:
            return False
        newInd = []
        for i in enumerate(self.indices[column_number]):
            self.indices[column_number][i] == None
        return True