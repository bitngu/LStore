import math
MAX_ENTRIES = 512
MAX_INPUT = 0xFFFFFFFFFFFFFFFF
class Page:

    def __init__(self):
        self.num_records = 0
        self.tps = 0
        self.data = bytearray(4096)
        self.isDirty = False
        self.path = ""
        self.pos = 0
        self.age = 0

    def has_capacity(self):
        return self.num_records < MAX_ENTRIES

    def half_read(self, location, is_upper):
        upper = 0 if is_upper else 4
        try:
            if location < 512:
                index  = (location * 8) + upper
                return int.from_bytes(self.data[index:index + 4], 'big')
            return False 
        except:
            return False 

    def read(self, location):
        try:
            if location < 512:
                index  = location * 8
                data = int.from_bytes(self.data[index:index + 8], 'big')
                return data
            return False 
        except:
            return False 

    def half_write(self, value, location, is_upper, is_inc):
        upper = 0 if is_upper else 4
        try:
            # This is throwing an error for some reason
            if not self.has_capacity() and is_inc:
                return False 
            index = (location * 8) + upper
            self.data[index:index + 4] = value.to_bytes( 4, 'big')
            if is_inc:
                self.num_records += 1
            int.from_bytes(self.data[index:index + 4], 'big')
        except:
            print("ERROR")
            return False
        else:
            return location

    def write(self, value, location = None):
        
        try:
            if self.has_capacity() or location and location < 512:
                if location == None:
                    index = self.num_records * 8
                else:
                    index = location * 8
                self.data[index:index + 8] = value.to_bytes( 8, 'big')
                self.num_records += 1
                return math.floor(index/8)
            return False 
        except:
            return False

    def get_tps(self):
        return self.tps
        
    def set_tps(self, tps):
        self.tps = tps
        pass
