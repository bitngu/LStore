from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.pages = []
        self.index = Index(self)
        # Create pages and assign them to the self.pages list. Start with one for each column
        for i in range(0, num_columns):
            # Create a new page
            page = Page()
            # Add the page to the pages list
            self.pages.append(Page())
            # Add a reference to the column number in the page directory
            self.page_directory[i] = [page] # This is a list to allow multiple pages if ones fill up
        # There needs to be additional pages added to tie together disparate records I believe
        # These are the pages that we will use in the read to tie together all the pages and return the full record based
        # on the found slot in the page

        pass

    # Looks in the pages corresponding to the provided column number
    # Finds all slots in the pages that match
    # Fill out the record from all pages based on the slot that was matched, only including the selected columns
    # Returns all matching records or False if none
    def read(self, index_value, index_column, query_columns):
        pass

    # Adds a new slot to each column, adding the slot to the last open page in the page_directory for the column
    # Look inside the bookkeeping for each page and check for any 0 (meaning a slot is available)
    # If all pages in the page_directory for a column are full, create a new page and add it to that
    # Tie together the reference to each page so the full record can be retrieved
    def write(self, *columns):
        pass

    # Finds the record to be deleted based on the primary key and updates each page's bookkeeping to make 
    #   the previously used slots available.
    def delete(self, primary_key):
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
