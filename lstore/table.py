from lstore.index import Index
from lstore.page import Page
from time import time
import math

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
        self.index = Index(self)
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
        # Add new entries into the timestamp page
        timestampLoc = getEmptyPage(self.timestamp).write(time.time())
        # Check if current base page is full for each column and do the insert
        for i in range(0, columns.len()):
            # Find or create an empty page for base
            page = getEmptyPage(self.page_directory[i].base)
            # Perform the insert for that column
            recordLoc = page.write(columns[i])
            # Exit if the write did not work
            if not recordLoc:
                return False
        #RecordLoc should be the same across all columns
        num_base = (len(self.page_directory[0].base) - 1) * 512
        return self.set_meta( num_base + recordLoc)
        

    def update(self, *columns):
       # Check if current tail page is full for each column and do the insert
        record = self.locate_record(columns[self.key])
        if record:
            return False
        
        rid = record.rid - 1

        # Add new entries into the timestamp page
        timestampLoc = getEmptyPage(self.timestamp).write(time.time())

        for i in range(0, columns.len()):
            # Find or create an empty page for tail
            page = getEmptyPage(self.page_directory[i].tail)
            # Perform the insert for that column
            # check if colunms[i] is none
            # if None insert the record.column[i] into tail instead
            # store the location of where the tail was writen into tail_rid

        tail_rid = 0 # delete this line later
        self.meta_update(record.rid, tail_rid)
        pass


    def delete(self, primary_key):
        pass

    def __merge(self):
        print("merge is happening")
        pass

    def set_meta(self, location):
        # Add new entries into the RID page
        rid = getEmptyPage(self.RID)
        # Add new entries into the schema page
        schema = getEmptyPage(self.schema)
        # Write RID for new inserted record and initianize schema for record
        return rid.half_write(location, location % 512, True, True) and schema.write(0)

    def meta_update( self, rid, tail_rid):
        rid = rid - 1
        
        # get an indirection page
        ind_page = getEmptyPage(self.indirection)
        # write into indirection location of tail page for rid's record
        tail_loc = ind_page.half_write(tail_rid, ind_page.num_records, True, True)
        tail_loc = ((len(ind_page) - 1) * 512) + tail_loc
        # grab the previous ind location from the rid column 
        prev_ind = self.RID[math.floor(rid / 512)].half_read(rid % 512, False)
        # if there was something previously saved there save the previous location to the 
        # second half of the most recent indirection column
        if prev_ind > 0:
            ind_page.half_write(prev_ind, (tail_loc - 1) % 512, False, False)
        
        #save the location of the indirection into the second half of the rid col
        self.RID[math.floor(rid / 512)].half_write( tail_loc, rid % 512, False, False)
        self.schema[math.floor(rid / 512)].half_write( 1, rid % 512, False, False)

    def locate_record( self, key):
        # get the rid of the key given
        rid = self.locate_rid(key)
        if not rid:
            return False
        
        rid = rid - 1

        col = []
        # check the to see if the record has been modified
        is_mod = self.schema[math.floor(rid / 512)].read(rid % 512) > 0
        for i in range(0, self.num_columns):
            if is_mod:
                #if modified grab from tail
                ind = self.RID[math.floor(rid / 512)].half_read( rid % 512, False) - 1
                tail_rid = self.indirection[math.floor(ind / 512)].half_read(ind % 512, True) - 1
                val = self.page_directory[i].tail[math.floor(tail_rid / 512)].read(tail_rid % 512)
                col.append(val)
            else:
                #if not modified grab from base
                val = self.page_directory[i].base[math.floor(rid / 512)].read(rid % 512)
                col.append(val)

        return Record(rid + 1, key, col)

    def locate_rid( self, key):
        #set of pages we are looking through
        pages = self.page_directory[self.key]
        #loops through every rid page
        for i in range(0, len(self.RID)):
            rid_page = self.RID[i]
            #loops through every entr in rid page
            for j in range(0, rid_page.num_records):
                rid = rid_page.half_read(j, True) - 1
                #checks if schema has been modified
                if self.schema[math.floor(rid / 512)].read(rid % 512) == 0:
                    #It has not been modified so check location in base page
                    if pages.base[math.floor(rid / 512)].read(rid % 512) == key:
                        return rid + 1
                else:
                    # It has been modified so search for its location in the base page
                    ind = rid_page.half_read(j, False) - 1
                    tail_rid = self.indirection[math.floor(ind / 512)].half_read(ind % 512, True) - 1
                    if pages.tail[math.floor(tail_rid / 512)].read(tail_rid % 512) == key:
                        return rid + 1

        return False

    # Internal helper function for getting or creating an empty page
def getEmptyPage(pages):
    # Check if the last page is empty
    if not pages[-1].has_capacity():
        pages.append(Page())
    # Return the last page with space
    return pages[-1]

 
