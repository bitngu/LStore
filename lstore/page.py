MAX_ENTRIES = 512
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records <= MAX_ENTRIES

    def read(self, location):
        try:
            if location < 512:
                index  = location * 8
                return int.from_bytes(self.data[index:index + 8], 'big')
            return False 
        except:
            return False 

    def write(self, value):
        
        try:
            if self.has_capacity():
                index = self.num_records * 8
                self.data[index:index + 8] = value.to_bytes( 8, 'big')
                self.num_records += 1
                return True    
            return False 
        except:
            return False

