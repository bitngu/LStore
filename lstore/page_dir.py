from lstore.page import Page
import os
MAX_CAP = 1
class Directory:
    def __init__(self, num_cols):
        self.num_cols = num_cols
        self.dir = []
        self.emtries = 0
        for i in range(0, num_cols):
            self.dir.append({
                'base': [None],
                'tail': [None]
            })

    def getEmptyPage(self, column, type):
        page = self.dir[column][type]
        if  page[-1] == None:
            #Temp solution Until Evict
            page[-1] = Page()
            #if self.entries == MAX_CAP:
                #pass #perform eviction
            #grab page from file and return it
        
        if page[-1].has_capacity():
            return page[-1]
        else:
            #self.generate_page(column, type)
            page.append(Page())
            return page[-1]

    def grab_page(self, column, type, location):
        page = self.dir[column][type]
        if  page[location] == None:
            if self.entries == MAX_CAP:
                pass #perform eviction
            #grab page from file and return it
            pass
        else:
            return page[location]

    def set_as_new( self, path):
        #loop through every col and set up their empty tail and base pages
        for i in range(0, self.num_cols):
            base_path = os.path.join(path, str(i) + "_base.bin")
            tail_path = os.path.join(path, str(i) + "_tail.bin")
            page = Page()
            base_file = open( base_path, 'wb')
            base_file.write(page.data)
            base_file.close()
            tail_file = open( tail_path, 'wb')
            tail_file.write(page.data)
            tail_file.close()
            
    def set_as_old( self, path):
        #
        base_path = os.path.join(path, "0_base.bin")
        tail_path = os.path.join(path, "0_tail.bin")
        num_base = os.path.getsize(base_path) / 4096
        num_tail = os.path.getsize(tail_path) / 4096
        for i in range(0, self.num_cols):
            self.dir[i]['base'] = [None]*num_base
            self.dir[i]['tail'] = [None]*num_tail


class Meta:
    def __init__(self):
        self.data = [None]
        self.entries = 0
        self.file_path = None

    def getEmptyPage(self):
        if self.data[-1] == None:
            #Temp solution Until Evict
            self.data[-1] = Page()
            if self.entries == MAX_CAP:
                #perform Eviction
                pass
            #grab page from file
            pass
        if self.data[-1].has_capacity():
                return self.data[-1]
        else:
            #self.generate_page()
            self.data.append(Page())
            return self.data[-1]

    def grab_page(self, location):
        
        if self.data[location] == None:
            self.data[location] = Page()
            if self.entries == MAX_CAP:
                #perform Eviction
                pass
            #grab page from file
            pass
        return self.data[location]

    def set_as_new(self, path, file_name):
        self.file_path = os.path.join(path, file_name)
        page = Page()
        file = open( self.file_path, 'wb')
        file.write(page.data)
        file.close()


    def set_as_old(self, path, file_name):
        file_path = os.path.join(path, file_name)
        file_size = os.path.getsize(file_path) / 4096
        self.data = [None]*file_size

def gen_empty(file_path):
    file_size = os.path.getsize(file_path)
    page = Page()
    file = open(file_path, "wb")
    file.seek(file_size)
    file.write(page.data)
    file.close()
    page.path = file_path
    page.pos = file_size