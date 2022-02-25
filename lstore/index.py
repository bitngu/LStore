"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index:

    def init(self, table):
        # One index for each table. All our empty initially.
        self.indices = [] * len(table.page_directory)
        self.RID = table.RID
        self.indirection = table.indirection
        self.schema = table.schema
        self.key = table.key

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if column_number >= len(self.indices) or column_number < 0:
            return False
        newInd = []
        count = 0

        for i in self.indices[column_number]:
            newInd.append(count)
            count += 1
        self.indices[column_number] = newInd

        return True
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