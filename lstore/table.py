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
        self.page_directory = []
        self.index = Index(self)
        # Create pages and assign them to the self.pages list. Start with one for each column
        for i in range(0, num_columns):
            # Add a reference to the column number in the page directory
            self.page_directory.append({
                'base': [Page()],
                'tail': [Page()]
            })
        # Create RID page
        self.RID = [Page()]
        # Create Indirection page
        self.indirection = [Page()]
        # Create Schema page
        self.schema = [Page()]
        # Create Timestamp page
        self.timestamp = [Page()]
        pass

    def read(self, index_value, index_column, query_columns):
        # Find the record in the index

        # Only check the pages that are specified in the query_columns
        for i in query_columns.len():
            # Check if we want that column to be returned
            if query_columns[i] == 1:
                # Find the data
                print('just here to stop errors')
        pass

    # Adds a new slot to each column, adding the slot to the base page in the page_directory for the column
    # If all pages in the page_directory for a column are full, create a new page and add it to that
    # Tie together the reference to each page so the full record can be retrieved
    def write(self, *columns):
        # Add new entries into the RID page
        rid = getEmptyPage(self.RID)
        # Add new entries into the Indirection page
        indirection = getEmptyPage(self.indirection)
        # Add new entries into the schema page
        schema = getEmptyPage(self.schema)
        # Add new entries into the timestamp page
        timestampLoc = getEmptyPage(self.timestamp).write(time.time())
        # Check if current base page is full for each column and do the insert
        for i in range(0, columns.len()):
            # Find or create an empty page for base
            page = getEmptyPage(self.page_directory[i].base)
            # Perform the insert for that column
            record = page.write(columns[i])
            # Exit if the write did not work
            if not record:
                return False
            # 
        pass

    def update(self, *columns):
       # Check if current tail page is full for each column and do the insert
        for i in range(0, columns.len()):
            # Find or create an empty page for tail
            page = getEmptyPage(self.page_directory[i].tail)
            # Perform the insert for that column

        pass


    def delete(self, primary_key):
        pass

    def __merge(self):
        print("merge is happening")
        pass

    # Internal helper function for getting or creating an empty page
def getEmptyPage(pages):
    # Check if the last page is empty
    if not pages[-1].has_capacity():
        pages.append(Page())
    # Return the last page with space
    return pages[-1]

 
