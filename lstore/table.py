from lstore.index import Index
from lstore.page import Page
from datetime import datetime
from threading import Thread, Timer
from lstore.page_dir import Directory, Meta
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
    def __init__(self, name, num_columns, key, path, isNew):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.RID = Meta()
        # Create Indirection page
        self.indirection = Meta(self.RID)
        # Create Schema page
        self.schema = Meta()
        # Create Timestamp page
        #self.timestamp = Meta()
        self.page_directory = Directory(num_columns, path, self.RID, self.indirection, self.schema)
        
        # Assign index reference
        self.index = Index(self)

        if isNew:
            self.page_directory.set_as_new(path)
            self.RID.set_as_new(path, "RID.bin")
            self.indirection.set_as_new(path, "indirection.bin")
            self.schema.set_as_new(path, "schema.bin")
        else:
            self.page_directory.set_as_old(path)
            self.RID.set_as_old(path, "RID.bin")
            self.indirection.set_as_old(path, "indirection.bin")
            self.schema.set_as_old(path, "schema.bin")


    # Finds all records matching index_value in the column index_column. Only returns values from query_column
    def read(self, index_value, index_column, query_columns):
        # Find the records
        records = self.locate_record(index_value, index_column, query_columns)
        # Validate that records were found
        if not records:
            return False
        else:
            return records

    # Adds a new slot to each column, adding the slot to the base page in the page_directory for the column
    # If all pages in the page_directory for a column are full, create a new page and add it to that
    # Tie together the reference to each page so the full record can be retrieved
    def write(self, columns):
        # Add new entries into the timestamp page
        # timestampLoc = getEmptyPage(self.timestamp).write(time.time())
        # Make sure primary key is unique
        if self.locate_record(columns[self.key]):
            return False
        # Check if current base page is full for each column and do the insert
        for i in range(0, len(columns)):
            # Find or create an empty page for base
            page = self.page_directory.getEmptyPage(i, 'base')
            # Perform the insert for that column
            recordLoc = page.write(columns[i])
            # Exit if the write did not work
            if recordLoc == None:
                return False
        # RecordLoc should be the same across all columns
        num_base = (len(self.page_directory.dir[0]['base']) - 1) * 512
        return self.set_meta( num_base + recordLoc)
        

    def update(self, primary_key, columns):
        # Find the record to be updated by the primary key
        record = self.locate_record(primary_key)
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
        for i in range(0, len(columns)):
            # Find or create an empty page for tail
            page = self.page_directory.getEmptyPage(i, 'tail')#getEmptyPage(self.page_directory[i]['tail'])
            # If column value is None, insert the record.column[i] into tail instead
            if columns[i] == None:
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


    def __merge(self):
        print("merge is happening")
        pass

    # Add the metadata for a new record in the rid and schema pages
    def set_meta(self, location):
        # Add new entries into the RID page
        rid = self.RID.getEmptyPage()
        # Add new entries into the schema page
        schema = self.schema.getEmptyPage()
        # Write RID for new inserted record and initialize schema for record
        return rid.half_write(location, (location - 1) % 512, True, True) and schema.write(0)

    # Updates a record based on the rid and the rid of the tail+
    def meta_update(self, rid, tail_rid):
        # Get correct rid reference
        rid = rid - 1
        # Get an indirection page
        ind_page = self.indirection.getEmptyPage()
        tail_size = len(self.page_directory.dir[0]['tail']) - 1
        # write into indirection the location of tail page for rid's record
        tail_loc = ind_page.half_write((tail_size * 512) + tail_rid, ind_page.num_records, True, True)
        # Calculate the location of the new tail
        tail_loc = ((len(self.indirection.data) - 1) * 512) + tail_loc
        # *** This will be moved out for transactions in milestone 3 ***
        # Grab the previous ind location from the rid column
        rid_page = self.RID.grab_page(math.floor(rid / 512))
        prev_ind = rid_page.half_read(rid % 512, False)
        # if there was something previously saved there save the previous location to the 
        # second half of the most recent indirection column
        if prev_ind > 0:
            ind_page.half_write(prev_ind, (tail_loc - 1) % 512, False, False)
        # Update the location of the indirection in the second half of the rid column
        rid_page.half_write( tail_loc, rid % 512, False, False)
        schema_page = self.schema.grab_page(math.floor(rid / 512))
        ret = schema_page.half_write( 1, rid % 512, False, False)
        return True
    
    # Gets a full record based on primary key or multiple records based on specific column
    def locate_record(self, key, columnIndex = None, selectColumns = None):
        # Find the rids based on the columnIndex
        rids = self.locate_rid(key, columnIndex)
        print(rids)
        # List of records to return
        # Rid was not found returns false
        if not rids:
            return False
        records = []
        # Find the record for each rid
        for rid in rids:
            # Error if no rid was found
            if not rid:
                return False
            # Calculate the correct rid
            rid = rid - 1
            
            # Columns associated with the record here
            col = []
            # check the to see if the record has been modified
            schema_page = self.schema.grab_page(math.floor(rid / 512))
            is_mod = schema_page.read(rid % 512) > 0 
            # Get data from base or tail of each column
            for i in range(0, self.num_columns):
                # Skip the column if it is not selected to return
                if selectColumns != None and selectColumns[i] == 0:
                    # Add none if the column is not to be returned
                    col.append(None)
                    continue
                # If modified grab from tail
                if is_mod:
                    # Get the indirection RID
                    rid_page = self.RID.grab_page(math.floor(rid / 512))
                    ind = rid_page.half_read( rid % 512, False) - 1
                    # Find the RID of the tail
                    ind_page = self.indirection.grab_page(math.floor(ind / 512))
                    tail_rid = ind_page.half_read(ind % 512, True) - 1
                    # Read the value from the tail
                    val_page = self.page_directory.grab_page(i, 'tail', math.floor(tail_rid / 512))
                    val = val_page.read(tail_rid % 512)
                    # Add the value to the record columns to be returned
                    col.append(val)
                else: # If not modified grab from base
                    # Get the value from the base record
                    val_page = self.page_directory.grab_page(i, 'base', math.floor(rid / 512))
                    val = val_page.read(rid % 512)
                    # Add the value to the record columns to be returned
                    col.append(val)
            # Save the record with populated columns to the list
            records.append(Record(rid + 1, key, col))
            #records.append(col)
        
        # Check if the search was performed on the primary key
        if columnIndex == None:
            # Return only one record for the primary key
            return records[0]
        else:
            # Return all records for all other columns
            return records

    # Finds a record's rid based on the primary key
    def locate_rid(self, key, index = None):
        # Default index to primary key
        if not index:
            index = self.key
        # Set of pages we are looking through
        # Array of rids
        rids = []
        # Loops through every rid page
        for i in range(0, len(self.RID.data)):
            # Exit if primary key has already been found
            if index == self.key and not len(rids) == 0:
                break
            rid_page = self.RID.grab_page(i)
            print("Number of records:")
            print(rid_page.num_records)
            # Loops through every entry in rid page
            for j in range(0, rid_page.num_records):
                # Exit if primary key has already been found
                if index == self.key and not len(rids) == 0:
                    break
                rid = rid_page.half_read(j, True) - 1
                # Checks if RID has been deleeted  and skips this rid
                if rid == 0xFFFFFFFF - 1:
                    continue
                #checks if schema has been modified
                schema_page = self.schema.grab_page(math.floor(rid / 512))
                is_mod = schema_page.read(rid % 512) == 0
                if is_mod:
                    # It has not been modified so check location in base page
                    page = self.page_directory.grab_page( index, 'base', math.floor(rid / 512))
                    if page.read(rid % 512) == key:
                        rids.append(rid + 1)
                else:
                    # It has been modified so search for its location in the indirection page
                    ind = rid_page.half_read(j, False) - 1
                    # Get the rid of the tail from the indirection pages
                    ind_page = self.indirection.grab_page(math.floor(ind / 512))
                    tail_rid = ind_page.half_read(ind % 512, True) - 1
                    page = self.page_directory.grab_page( index, 'tail', math.floor(tail_rid / 512))
                    if page.read(tail_rid % 512) == key:
                        rids.append(rid + 1)
        # Exit with error if no rids were found
        print(rids)
        if not rids or not rids[0]:
            return False
        else:
            return rids

    def locate_range(self, start, end, index):
        found = []
        # Loop through every RID page
        for i in range(0, len(self.RID.data)):
            # Grab the current RID page we are working on
            rid_page = self.RID.grab_page(i)
            #loop through every entry in the current RID page
            for j in range(0, rid_page.num_records):
                # Grab the current RID and Check if it is deleted
                rid = rid_page.half_read(j, True) - 1
                if rid == 0xFFFFFFFF - 1:
                    continue

                # Check if the RID has been modefied
                schema_page = self.schema.grab_page(math.floor(rid / 512))
                if schema_page.read(rid % 512) == 0:
                    # It has not been modified so check location in base page
                    key_page = self.page_directory.grab_page(self.key, 'base', math.floor(rid / 512))
                    k_val = key_page.read(rid % 512)
                    # Check if it is in range and append it to the list to return
                    if k_val >= start and k_val <= end:
                        val_page = self.page_directory.grab_page(index, 'base', math.floor(rid / 512))
                        val = val_page.read(rid % 512)
                        found.append(val)
                else:
                    # It has been modified so search for its location in the indirection page
                    ind = rid_page.half_read(j, False) - 1
                    # Get the rid of the tail from the indirection pages
                    ind_page = self.indirection.grab_page(math.floor(ind / 512))
                    tail_rid = ind_page.half_read(ind % 512, True) - 1
                    key_page = self.page_directory.grab_page(self.key, 'tail', math.floor(tail_rid / 512))
                    k_val = key_page.read(tail_rid % 512)
                    # check if it is in range and append it to the list to return
                    if k_val >= start and k_val <= end:
                        val_page = self.page_directory.grab_page(index, 'tail', math.floor(tail_rid / 512))
                        val = val_page.read(tail_rid % 512)
                        found.append(val)
        return found

    def save(self):

        self.RID.save()
        # Create Indirection page
        self.indirection.save()
        # Create Schema page
        self.schema.save()
        self.page_directory.save()
