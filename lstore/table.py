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
        # Assign index reference
        self.index = Index(self)
        pass

    def read(self, index_value, index_column, query_columns):
        # *** This returns all values where the index_value matches the value in index_column.
        # *** locate_record only returns one record, so we either have to modify it, or
        # *** create a new helper function.
        # Find the record in the index

        # Only check the pages that are specified in the query_columns
        for i in range(0, query_columns.len()):
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
        #timestampLoc = getEmptyPage(self.timestamp).write(time.time())
        # Check if current base page is full for each column and do the insert
        for i in range(0, len(columns)):
            # Find or create an empty page for base
            page = getEmptyPage(self.page_directory[i]['base'])
            # Perform the insert for that column
            recordLoc = page.write(columns[i])
            # Exit if the write did not work
            if not recordLoc:
                return False
        # RecordLoc should be the same across all columns
        num_base = (len(self.page_directory[0]['base']) - 1) * 512
        return self.set_meta( num_base + recordLoc)
        
    def update(self, *columns):
        # Find the record to be updated by the primary key
        record = self.locate_record(columns[self.key])
        # Error if no record is found
        if not record:
            return False
        # Get the correct rid for the record
        rid = record.rid - 1
        # Update the timestamp page
        # timestampLoc = getEmptyPage(self.timestamp).write(time.time())
        # Location of updated tail
        tail_rid = None
        # Update the columns and pass to meta_update on completion
        for i in range(0, columns.len()):
            # Find or create an empty page for tail
            page = getEmptyPage(self.page_directory[i]['tail'])
            # If column value is None, insert the record.column[i] into tail instead
            if not columns[i]:
                updatedLoc = page.write(record.columns[i])
            else: # Otherwise insert the new value
                updatedLoc = page.write(columns[i])
            # Save or check the location to be written to the tail
            if not tail_rid:
                # Initialize tail_rid
                tail_rid = updatedLoc
            else :
                # Check to make sure that the tail_rid matches the returned location
                if tail_rid != updatedLoc:
                    return False
        # *** This maybe should be used in transactions to commit the changes ***
        # Update the record's metadata
        return self.meta_update(record.rid, tail_rid)

    def delete(self, primary_key):
        pass

    def __merge(self):
        print("merge is happening")
        pass

    # Add the metadata for a new record in the rid and schema pages
    def set_meta(self, location):
        # Add new entries into the RID page
        rid = getEmptyPage(self.RID)
        # Add new entries into the schema page
        schema = getEmptyPage(self.schema)
        # Write RID for new inserted record and initialize schema for record
        return rid.half_write(location, (location - 1) % 512, True, True) and schema.write(0)

    # Updates a record based on the rid and the rid of the tail+
    def meta_update(self, rid, tail_rid):
        # Get correct rid reference
        rid = rid - 1
        # Get an indirection page
        ind_page = getEmptyPage(self.indirection)
        # write into indirection the location of tail page for rid's record
        tail_loc = ind_page.half_write(tail_rid, ind_page.num_records, True, True)
        # Calculate the location of the new tail
        tail_loc = ((len(ind_page) - 1) * 512) + tail_loc
        # *** I think this should maybe be split out into transactions, should only override data on transaction commit ***
        # Grab the previous ind location from the rid column
        prev_ind = self.RID[math.floor(rid / 512)].half_read(rid % 512, False)
        # if there was something previously saved there save the previous location to the 
        # second half of the most recent indirection column
        if prev_ind > 0:
            ind_page.half_write(prev_ind, (tail_loc - 1) % 512, False, False)
        # Update the location of the indirection in the second half of the rid column
        self.RID[math.floor(rid / 512)].half_write( tail_loc, rid % 512, False, False)
        self.schema[math.floor(rid / 512)].half_write( 1, rid % 512, False, False)
        # Return True on success
        return True
    
    # *** Only returns one record by primary key *** 
    # Gets a full record based on primary key
    def locate_record(self, key):
        # get the rid of the key given
        rid = self.locate_rid(key)
        # Error if no rid was found
        if not rid:
            return False
        # Calculate the correct rid
        rid = rid - 1
        # Columns associated with the record here
        col = []
        # check the to see if the record has been modified
        is_mod = self.schema[math.floor(rid / 512)].read(rid % 512) > 0
        # Get data from base or tail of each column
        for i in range(0, self.num_columns):
            # If modified grab from tail
            if is_mod:
                # Get the indirection RID
                ind = self.RID[math.floor(rid / 512)].half_read( rid % 512, False) - 1
                # Find the RID of the tail
                tail_rid = self.indirection[math.floor(ind / 512)].half_read(ind % 512, True) - 1
                # Read the value from the tail
                val = self.page_directory[i]['tail'][math.floor(tail_rid / 512)].read(tail_rid % 512)
                # Add the value to the record columns to be returned
                col.append(val)
            else: # If not modified grab from base
                # Get the value from the base record
                val = self.page_directory[i]['base'][math.floor(rid / 512)].read(rid % 512)
                # Add the value to the record columns to be returned
                col.append(val)
        # Return the record with populated columns
        return Record(rid + 1, key, col)

    # Finds a record's rid based on the primary key
    def locate_rid(self, key):
        #set of pages we are looking through
        pages = self.page_directory[self.key]
        #loops through every rid page
        for i in range(0, len(self.RID)):
            rid_page = self.RID[i]
            #loops through every entry in rid page
            for j in range(0, rid_page.num_records):
                rid = rid_page.half_read(j, True) - 1
                #checks if schema has been modified
                if self.schema[math.floor(rid / 512)].read(rid % 512) == 0:
                    # It has not been modified so check location in base page
                    if pages['base'][math.floor(rid / 512)].read(rid % 512) == key:
                        return rid + 1
                else:
                    # It has been modified so search for its location in the indirection page
                    ind = rid_page.half_read(j, False) - 1
                    # Get the rid of the tail from the indirection pages
                    tail_rid = self.indirection[math.floor(ind / 512)].half_read(ind % 512, True) - 1
                    if pages['tail'][math.floor(tail_rid / 512)].read(tail_rid % 512) == key:
                        return rid + 1
        # Exit with error if no rid was found
        return False

# Internal helper function for getting or creating an empty page
def getEmptyPage(pages):
    # Check if the last page is empty
    if not pages[-1].has_capacity():
        pages.append(Page())
    # Return the last page with space
    return pages[-1]

 
