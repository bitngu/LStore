from lstore.table import Table
from lstore.page import Page
import os

class Database():

    def __init__(self):
        self.tables = {}
        self.path = None
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        pass
        #check if path exist if it does not exist create a directory
        if not os.path.isdir(path):
            os.makedirs(path)
        #Finds every subdirectory in the given path
        #And creates its appropriate table
        for i in os.walk(path):
            for j in i[1]:
                self.grab_table(j, os.path.join(i[0], j))

    def close(self):
        for i in os.walk(self.path):
            for j in i[1]:
                self.save_table(j)

    def save_table(self, name):
        table = self.get_table(name)
        table.save()

    #Given name and Path of table it generates a table 
    def grab_table(self, name, path):
        #grab metafile contains number of columns and key column
        meta_file = open( os.path.join(path, "meta.bin"), 'rb')
        meta_page = Page()
        meta_page.data = meta_file.read(4096)
        meta_file.close()
        self.tables[name] = Table(name, meta_page.read(0), meta_page.read(1), path, False)
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if not self.path == None:
            table_path = os.path.join(self.path, name)
            if not os.path.isdir(table_path):
                os.makedirs(table_path)
            meta_page = Page()
            meta_page.write(num_columns)
            meta_page.write(key_index)
            meta_path = os.path.join(table_path, "meta.bin")
            print(meta_path)
            mfile = open(meta_path, "wb")
            mfile.write(meta_page.data)
            mfile.close()
            table = Table(name, num_columns, key_index, table_path, True)
            self.tables[name] = table
        
            return table
        return False

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            #Delete sub directory of table 
        

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name in self.tables:
            return self.tables[name]
        return None
