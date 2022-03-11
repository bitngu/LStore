from lstore.page import Page
import os
import math
MAX_CAP = 1
class Directory:
    def __init__(self, num_cols):
        self.num_cols = num_cols
        self.dir = []
        self.emtries = 0
        self.path = None
        for i in range(0, num_cols):
            self.dir.append({
                'base': [],
                'tail': []
            })

    def save(self):
        for i in range(0, self.num_cols):
            base_path = os.path.join(self.path, str(i) + "_base.bin")
            tail_path = os.path.join(self.path, str(i) + "_tail.bin")
            base_file = open( base_path, 'wb')
            tail_file = open( tail_path, 'wb')
            
            for j in range(0, len(self.dir[i]['base'])):
                if  self.dir[i]['base'][j] == None:
                    break
                base_file.write(self.dir[i]['base'][j].data)

            for k in range(0, len(self.dir[i]['tail'])):
                if  self.dir[i]['tail'][k] == None:
                    break
                tail_file.write(self.dir[i]['tail'][k].data)

            base_file.close()
            tail_file.close()
            print("Finished saving to " + base_path + " and to " + tail_path)

    def getEmptyPage(self, column, type):
        page = self.dir[column][type]
        if  page[-1] == None:
            page[-1] = Page()
        
        if page[-1].has_capacity():
            return page[-1]
        else:
            page.append(Page())
            return page[-1]

    def grab_page(self, column, type, location):
        page = self.dir[column][type]
        if  page[location] == None:
            self.dir[column][type][location] = Page()
        else:
            return page[location]

    def set_as_new( self, path):
        #loop through every col and set up their empty tail and base pages
        self.path = path
        for i in range(0, self.num_cols):
            base_path = os.path.join(path, str(i) + "_base.bin")
            tail_path = os.path.join(path, str(i) + "_tail.bin")
            base_file = open( base_path, 'wb')
            base_file.close()
            tail_file = open( tail_path, 'wb')
            tail_file.close()
            self.dir[i]['base'].append(None)
            self.dir[i]['tail'].append(None)

            
    def set_as_old( self, path):
        self.path = path
        base_path = os.path.join(path, "0_base.bin")
        tail_path = os.path.join(path, "0_tail.bin")
        num_base = math.floor(os.path.getsize(base_path) / 4096)
        num_tail = math.floor(os.path.getsize(tail_path) / 4096)
        for i in range(0, self.num_cols):
            
            base_file_path = os.path.join(path, str(i) + '_base.bin')
            b_file = open(base_file_path, 'rb')

            for j in range(0, num_base):
                base = Page()
                base.data = b_file.read(4096)
                base.num_records = 512
                self.dir[i]['base'].append(base)

            b_file.close()
            self.dir[i]['base'].append(None)
            tail_file_path = os.path.join(path, str(i) + '_tail.bin')
            t_file = open( tail_file_path, 'rb')

            for k in range(0, num_tail):
                tail = Page()
                tail.data = t_file.read(4096)
                tail.num_records = 512
                self.dir[i]['tail'].append(tail)

            t_file.close()
            self.dir[i]['tail'].append(None)


class Meta:
    def __init__(self):
        self.data = []
        self.entries = 0
        self.file_path = None

    def save(self):
        file = open(self.file_path, 'wb')
        for i in range(0, len(self.data)):
            if(self.data[i] == None):
                break
            file.write(self.data[i].data)
        file.close()
        print('finished saving to ' + self.file_path)

    def getEmptyPage(self):
        if self.data[-1] == None:
            self.data[-1] = Page()
            
        if self.data[-1].has_capacity():
                return self.data[-1]
        else:
            self.data.append(Page())
            return self.data[-1]

    def grab_page(self, location):
        # Adding new pages if needed
        if len(self.data) <= location:
            self.data.append(Page())
        return self.data[location]

    def set_as_new(self, path, file_name):
        self.file_path = os.path.join(path, file_name)
        #page = Page()
        file = open( self.file_path, 'wb')
        #file.write(page.data)
        file.close()
        self.data.append(None)


    def set_as_old(self, path, file_name):
        self.file_path = os.path.join(path, file_name)
        file_size = math.floor(os.path.getsize(self.file_path) / 4096)
        #self.data = [None]*file_size
        file = open(self.file_path, 'rb')
        for i in range(0, file_size):
            page = Page()
            page.data = file.read(4096)
            page.num_records = 512
            self.data.append(page)
            
        self.data.append(None)


def gen_empty(file_path):
    file_size = os.path.getsize(file_path)
    page = Page()
    file = open(file_path, "wb")
    file.seek(file_size)
    file.write(page.data)
    file.close()
    page.path = file_path
    page.pos = file_size