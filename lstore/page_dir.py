from lstore.page import Page
import os
import math
MAX_CAP = 1

class Meta:
    def __init__(self, rid = None):
        self.data = [None]
        self.entries = 0
        self.file_path = None
        self.rid = rid

    def save(self):
        file = open(self.file_path, 'wb')
        for i in range(0, len(self.data)):
            if(self.data[i].isDirty):
                file.seek(i * 4096)
                file.write(self.data[i].data)
        file.close()

    def getEmptyPage(self):
        if self.data[-1] == None:
            #Temp solution Until Evict
            #self.data[-1] = Page()
            if self.entries == MAX_CAP:
                #perform Eviction
                pass
            #grab page from file
            file_pos = os.path.getsize(self.file_path) - 4096
            file = open( self.file_path, 'rb')
            file.seek(file_pos)
            data = file.read(4096)
            file.close()
            oldpage = Page()
            oldpage.data = data
            oldpage.path = self.file_path
            oldpage.pos = file_pos
            oldpage.num_records = self.calc_cap(math.floor(file_pos / 4096))
            self.data[-1] = oldpage

        if self.data[-1].has_capacity():
                return self.data[-1]
        else:
            npage = gen_empty(self.file_path)
            self.data.append(npage)
            return self.data[-1]

    def calc_cap(self, file_pos):
        if not self.rid == None:
            return self.rid.calc_cap(file_pos)

        page = self.grab_page(file_pos)
        cap = 0
        for i in range(0, 512):
            if page.read(i) == 0:
                return cap
            cap = cap + 1
        return cap

    def grab_page(self, location):
        print(location, self.file_path)
        if self.data[location] == None:
            print("empty")
            self.data[location] = Page()
            #if self.entries == MAX_CAP:
                #perform Eviction
            #    pass
            #grab page from file
            file = open( self.file_path, 'rb')
            file.seek(location * 4096)
            
            oldpage = Page()
            oldpage.data = file.read(4096)
            file.close()
            oldpage.path = self.file_path
            oldpage.pos = location * 4096
            self.data[location] = oldpage
        return self.data[location]

    def set_as_new(self, path, file_name):
        self.file_path = os.path.join(path, file_name)
        page = Page()
        file = open( self.file_path, 'wb')
        file.write(page.data)
        file.close()


    def set_as_old(self, path, file_name):
        file_path = os.path.join(path, file_name)
        file_size = math.floor(os.path.getsize(file_path) / 4096)
        self.data = [None]*file_size



class Directory:
    def __init__(self, num_cols, path, rid, indirection, schema):
        self.num_cols = num_cols
        self.dir = []
        self.emtries = 0
        self.path = path
        self.RID = rid
        self.indirection = indirection
        self.schema = schema
        for i in range(0, num_cols):
            self.dir.append({
                'base': [None],
                'tail': [None]
            })

    def getEmptyPage(self, column, type):
        page = self.dir[column][type]
        if  page[-1] == None:
            #Temp solution Until
            #page[-1] = Page()
            #if self.entries == MAX_CAP:
                #pass #perform eviction
            #grab page from file and return it
            file_path = os.path.join(self.path, str(column) + '_' +  type + '.bin')
            file_pos = os.path.getsize(file_path) - 4096
            file = open( file_path, 'rb')
            file.seek(file_pos)
            data = file.read(4096)
            file.close()
            oldpage = Page()
            oldpage.data = data
            oldpage.path = file_path
            oldpage.pos = file_pos
            oldpage.num_records = self.calc_cap(type, file_pos)
            page[-1] = oldpage
            
        if page[-1].has_capacity():
            return page[-1]
        else:
            path = os.path.join(self.path,  str(column) + "_" + type + ".bin")
            npage = gen_empty(path)
            page.append(npage)
            return page[-1]

    def calc_cap( self, type, file_pos):
        if type == 'basic':
            page = self.RID.grab_page(math.floor(file_pos / 4096))
        else:
            page = self.indirection.grab_page(math.floor(file_pos / 4096))
        
        cap = 0
        for i in range(0, 512):
            if page.read(i) == 0:
                return cap
            cap = cap + 1
        return cap

    def grab_page(self, column, type, location):
        print(location)
        page = self.dir[column][type]
        if  page[location] == None:
            if self.entries == MAX_CAP:
                pass #perform evicction
            #grab page from file and return it
            file_path = os.path.join(self.path, str(column) + '_' +  type + '.bin')
            file = open( file_path, 'rb')
            file.seek(location * 4096)
            data = file.read(4096)
            file.close()
            oldpage = Page()
            oldpage.data = data
            oldpage.path = file_path
            oldpage.pos = location * 4096
            page[location] = oldpage
        
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
        num_base = math.floor(os.path.getsize(base_path) / 4096)
        num_tail = math.floor(os.path.getsize(tail_path) / 4096)
        for i in range(0, self.num_cols):
            self.dir[i]['base'] = [None]*num_base
            self.dir[i]['tail'] = [None]*num_tail

    def save(self):
        for i in range(0, self.num_cols):
            base_path = os.path.join(self.path, str(i) + "_base.bin")
            tail_path = os.path.join(self.path, str(i) + "_tail.bin")
            base_file = open( base_path, 'wb')
            tail_file = open( tail_path, 'wb')
            
            for j in range(0, len(self.dir[i]['base'])):
                if  (not self.dir[i]['base'][j] == None) and self.dir[i]['base'][j].isDirty:
                    base_file.seek(j * 4096)
                    base_file.write(self.dir[i]['base'][j].data)

            for k in range(0, len(self.dir[i]['tail'])):
                if  (not self.dir[i]['tail'][k] == None) and self.dir[i]['tail'][k].isDirty:
                    tail_file.seek(k * 4096)
                    tail_file.write(self.dir[i]['tail'][k].data)

            base_file.close()
            tail_file.close()

def gen_empty(file_path):
    file_size = os.path.getsize(file_path)
    page = Page()
    file = open(file_path, "wb")
    file.seek(file_size)
    file.write(page.data)
    file.close()
    page.path = file_path
    page.pos = file_size
