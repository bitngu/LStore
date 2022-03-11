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
        # Create page directory dict
        self.page_directory = Directory(num_columns)
        # Create RID page
        self.RID = Meta()
        # Create Indirection page
        self.indirection = Meta()
        # Create Schema page
        self.schema = Meta()
        # Create Primary key lookup dict
        self.primaryKeyLookup = {}
        # Create Timestamp page
        #self.timestamp = Meta()
        # Assign index reference
        self.index = Index(self)
        # Set timer for running merge
        self.timer = self.merge_timer()
        # Handle new pages vs reading from files
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
            if not recordLoc:
                return False
        # RecordLoc should be the same across all columns
        num_base = (len(self.page_directory.dir[0]['base']) - 1) * 512
        # Add primary key to dict for fast lookup
        self.primaryKeyLookup[columns[self.key]] = num_base + recordLoc
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
            # Check if the column is the primary key
            if i == self.key and self.key:
                del self.primaryKeyLookup[record.key]
                self.primaryKeyLookup[columns[i]] = rid
        # *** This maybe should be used in transactions to commit the changes ***
        # Update the record's metadata
        return self.meta_update(record.rid, tail_rid)

    def __merge(self):
        # Create empty base update structure
        updated_bases = []
        # Update base page for each column
        for i in range(0, len(self.page_directory)):
            # Get the base pages
            bases = self.page_directory[i]['base']
            # Save the list of new base pages
            newBases = []
            # Read each base page
            for j in range(0, len(bases)):
                # Skip any base pages that could have had updates (are not full). Should only ever be the last base page
                if bases[j].has_capacity():
                    break
                # Create a new base page
                newBase = Page()
                tps = 0
                # Check all records in the base page
                for ridi in range(0, 512):
                    # Get the correct rid to search for
                    rid = j * 512 + ridi
                    # Do a lookup for all the columns
                    dict = self.locate_column_by_rid(rid, j)
                    # Append new columns to base page
                    newBase.write(dict['val'])
                    # Update tps if needed
                    if tps < dict['tail_rid']:
                        tps = dict['tail_rid']
                # Set the tps for the new base page
                newBase.set_tps(tps)
                # Update page_directory to reference new base page
                newBases.append(newBase)
            # Add new base pages to array to apply
            updated_bases.append(newBases)
        # Update the page_directory without overridding any new base pages that were created
        # *** Update for new page directory functionality ***
        for i in range(0, len(self.page_directory)):
            # Iterate over each updated base page
            for base in updated_bases[i]:
                # Update page_directory reference to new base pages
                self.page_directory[i]['base'][j] = base
        pass

    def merge(self):
        print("merge is happening")
        # Create a new thread to run the merge
        self.mergeWorker = Thread(target = self.__merge)
        # Start the thread
        self.mergeWorker.start()
        # Joins the thread with the main thread to wait for completion
        # worker.join()
        
    def merge_timer(self):
        print("Starting timer")
        # Immediately do a merge for testing
        #self.merge()
        # Seconds until next midnight
        s = 0
        # Get the current time object
        now = datetime.now()
        # Calculate hours until next midnight
        hours = int(now.strftime("%H"))
        # Increment seconds based on hour
        s += (23 - hours) * 60 * 60
        # Calculate minutes until next midnight
        minutes = int(now.strftime("%M"))
        # Increment seconds based on minutes
        s += (59 - minutes) * 60
        # Calculate seconds until next midnight
        seconds = 1 + int(now.strftime("%S"))
        # Increment seconds
        s += 59 - seconds
        # Validate current time
        if s <= 0:
            self.merge()
        # Create a new thread with the timer
        Timer(s, self.merge_timer).start()

    def abort(self, rid):
        # Iterate over each tail page
        for i in range(0, self.table.num_columns):
            for dir in self.table.page_directory[i]


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
                # Get the base page
                base = self.page_directory.grab_page(i, 'base', math.floor(rid / 512))
                # If modified grab from tail
                if is_mod:
                    # Get the indirection RID
                    rid_page = self.RID.grab_page(math.floor(rid / 512))
                    ind = rid_page.half_read( rid % 512, False) - 1
                    # Find the RID of the tail
                    ind_page = self.indirection.grab_page(math.floor(ind / 512))
                    tail_rid = ind_page.half_read(ind % 512, True) - 1
                    if tail_rid < base.get_tps():
                        val = base.read(rid % 512)
                    else:
                        # Read the value from the tail
                        val_page = self.page_directory.grab_page(i, 'tail', math.floor(tail_rid / 512))
                        val = val_page.read(tail_rid % 512)
                    # Add the value to the record columns to be returned
                    col.append(val)
                else: # If not modified grab from base
                    # Get the value from the base record
                    val = base.read(rid % 512)
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
    # Gets a full record based on rid
    def locate_column_by_rid(self, rid, i):
        # Calculate the correct rid
        rid = rid - 1
        # check the to see if the record has been modified
        is_mod = self.schema[math.floor(rid / 512)].read(rid % 512) > 0
        # Get the base page
        base = self.page_directory[i]['base'][math.floor(rid / 512)]
        # If modified grab from tail
        if is_mod:
            # Get the indirection RID
            ind = self.RID[math.floor(rid / 512)].half_read( rid % 512, False) - 1
            # Find the RID of the tail
            tail_rid = self.indirection[math.floor(ind / 512)].half_read(ind % 512, True) - 1
            # Check the tail_rid against the tps
            if tail_rid < base.get_tps():
                val = base.read(rid % 512)
            else: # Read the value from the tail
                val = self.page_directory[i]['tail'][math.floor(tail_rid / 512)].read(tail_rid % 512)
            # Read the value from the tail and return the tail rid
            return {
                "val": val,
                "tail_rid": tail_rid
            }
        else: # If not modified grab from base
            # Get the value from the base record
           return {
               "val": base.read(rid % 512),
               "tail_rid": 0
           }

    # Finds a record's rid based on the primary key
    def locate_rid(self, key, index = None):
        # Expedite primary key lookup
        if (not index) or (index == self.key):
            if key in self.primaryKeyLookup:
                return [self.primaryKeyLookup[key]]
            return False
        # Set of pages we are looking through
        # Array of rids
        rids = []
        # Loops through every rid page
        for i in range(0, len(self.RID.data)):
            # Exit if primary key has already been found
        
            if index == self.key and not len(rids) == 0:
                break
            rid_page = self.RID.grab_page(i)
            if rid_page == None:
                continue
            # Loops through every entry in rid page
            for j in range(0, rid_page.num_records):
                
                # Exit if primary key has already been found
                if index == self.key and not len(rids) == 0:
                    break
                rid = rid_page.half_read(j, True) - 1
                # Checks if RID has been deleted  and skips this rid
                if rid == 0xFFFFFFFF - 1:
                    continue

                if rid == -1:
                    break
                #checks if schema has been modified
                schema_page = self.schema.grab_page(math.floor(rid / 512))
                is_mod = schema_page.read(rid % 512) == 0
                # Get the base page
                base = self.page_directory.grab_page( index, 'base', math.floor(rid / 512))
                # Pull from tail if modified, base if not
                if is_mod:
                    # It has not been modified so check location in base page
                    if base.read(rid % 512) == key:
                        rids.append(rid + 1)
                else:
                    # It has been modified so search for its location in the indirection page
                    ind = rid_page.half_read(j, False) - 1
                    # Get the rid of the tail from the indirection pages
                    ind_page = self.indirection.grab_page(math.floor(ind / 512))
                    tail_rid = ind_page.half_read(ind % 512, True) - 1
                     # Check the tail_rid against the tps
                    if tail_rid < base.get_tps():
                        # Check base since merge has happened
                        if base.read(rid % 512) == key:
                            rids.append(rid + 1)
                    else: 
                        # Check tail since no merge has happened
                        page = self.page_directory.grab_page( index, 'tail', math.floor(tail_rid / 512))
                        if page.read(tail_rid % 512) == key:
                            rids.append(rid + 1)
        # Exit with error if no rids were found
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
            if rid_page == None:
                continue
            #loop through every entry in the current RID page
            for j in range(0, rid_page.num_records):
                # Grab the current RID and Check if it is deleted
                rid = rid_page.half_read(j, True) - 1
                if rid == 0xFFFFFFFF - 1:
                    continue
                
                if rid == -1:
                    break
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

            

# Internal helper function for getting or creating an empty page
def getEmptyPage(pages):
    # Check if the last page is empty
    if not pages[-1].has_capacity():
        pages.append(Page())
    # Return the last page with space
    return pages[-1]

 
