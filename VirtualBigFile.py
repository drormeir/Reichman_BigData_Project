from MockObjectStorage import MockObjectStorage
from bisect import bisect_left
import atexit

KB = 2**10
MB = KB*KB
GB = MB*KB
EOL = b'\n' # end of line

objectStorage = MockObjectStorage()

class VirtualBigFile:
    defaultBlockSize = 1*MB
    
    def __init__(self, name:str, blocksize=0, verboseOpen=0):
        assert len(name) > 0
        splited_name = name.split('.')
        assert len(splited_name) > 1, "File name must have an extension"
        self.name            = '.'.join(splited_name[:-1])
        self.extension       = "." + splited_name[-1]
        self.index_name      = name + ".index.csv"
        self.blocksize       = blocksize if blocksize > 0 else VirtualBigFile.defaultBlockSize
        self.appendix        = bytes()
        self.physicalsize    = 0
        self.files           = []
        self.sizes           = []
        self.first_locations = []
        self.last_locations  = []
        self.type_           = None
        str_index            = objectStorage.readObject(self.index_name,[tuple])
        if str_index is None:
            self.next_file_id = 0
        else:
            for csv_row in str_index:
                filename, size = csv_row[0], int(csv_row[1])
                self.files.append(filename)
                self.sizes.append(size)
                self.first_locations.append(self.physicalsize)
                self.physicalsize += size
                self.last_locations.append(self.physicalsize-1)
            last_name   = self.files[-1].split('.')
            # for example: last_name=['file' 'name' '999' 'extension'] --> last_name[-2] == '999'
            last_number = int(last_name[-2])
            self.next_file_id = last_number + 1
        self.old_index       = self.physicalsize > 0
        self.new_index       = False
        if verboseOpen>1 or verboseOpen==1 and self.physicalsize:
            print("VirtualBigFile.open({}) size={}".format(name,self.physicalsize))
        atexit.register(self.flush)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()
            
    def num_partitions(self):
        return len(self.files) + int(len(self.appendix) > 0)
    
    def append(self,data):
        is_list = isinstance(data,list)
        if isinstance(data,str) or is_list and (isinstance(data[0],str) or isinstance(data[0],tuple) and isinstance(data[0][0],str)):
            self.type_ = str
            self.appendix += bytearray(MockObjectStorage.convertStr2Bytes(data))
        elif is_list:
            for d in data:
                self.appendix += bytearray(d)
        else:
            self.appendix += bytearray(data)

    def readPartition(self, indPartition, type_=None):
        if indPartition < 0 or indPartition > len(self.files):
            return None
        if indPartition < len(self.files):
            return self.readData(self.first_locations[indPartition],self.last_locations[indPartition]+1, type_)
        return self.readData(self.physicalsize,self.physicalsize + len(self.appendix), type_)
    
    def readData(self,indStart=None,indEnd=None, type_=None):
        totalsize = self.physicalsize + len(self.appendix)
        if indStart is None:
            indStart = 0
        elif indStart < 0:
            indStart += totalsize
        if indEnd is None:
            indEnd = totalsize
        elif indEnd < 0:
            indEnd += totalsize
        if indStart >= indEnd or indStart < 0 or indEnd > totalsize:
            return None
        indFirstBlock = bisect_left(self.last_locations, indStart)
        indLastBlock  = bisect_left(self.last_locations, indEnd-1)
        if indFirstBlock < len(self.files):
            res             = objectStorage.readObject(self.files[indFirstBlock:min(indLastBlock+1,len(self.files))])
            indStartOfBlock = self.first_locations[indFirstBlock]
            indEndOfBlock   = self.totalsize if indLastBlock == len(self.files) else (self.last_locations[indLastBlock]+1)
        else:
            res             = []
            indStartOfBlock = self.physicalsize
            indEndOfBlock   = totalsize
        if indLastBlock == len(self.files):
            res.append(self.appendix)
        res = res[0] if len(res) == 1 else b''.join(res)
        if indStart > indStartOfBlock or indEnd < indEndOfBlock:
            res = res[(indStart-indStartOfBlock):(indEnd-indStartOfBlock)]
        if type_ is None:
            return res
        return MockObjectStorage.convertBytes2Str(res,type_)

    def flush(self, objectStorageFlush=False):
        self.writeAppendix()
        if self.new_index:
            if self.old_index:
                objectStorage.deleteObject(self.index_name)
            else:
                self.old_index = True
            # rebuilding index file on disk
            str_index = [name + ',' + str(size) for name,size in zip(self.files,self.sizes)]
            objectStorage.createObject(self.index_name,str_index)
            self.new_index = False
        if objectStorageFlush:
            objectStorage.flush([self.index_name] + self.files)
            
    def writeAppendix(self):
        len_appendix = len(self.appendix)
        if len_appendix < 1:
            return
        self.new_index = True
        indStart = 0
        while indStart + self.blocksize < len_appendix:
            if self.type_ is None or self.type_ != str:
                filesize = self.blocksize
            else:
                last_EOL_pos = self.appendix.rfind(EOL,indStart,indStart + self.blocksize)
                if last_EOL_pos < 0:
                    last_EOL_pos = self.appendix.rfind(EOL,indStart,len_appendix)
                    if last_EOL_pos < 0:
                        # did not find any end-of-line till the end of appendix
                        break                        
                filesize = last_EOL_pos + 1
            self.__writePartition__(indStart,filesize)
            indStart += filesize
        if indStart < len_appendix:
            # writing the rest of last partition
            self.__writePartition__(indStart,len_appendix-indStart)
        self.appendix = bytearray()
        
    def __writePartition__(self,indStart,blocksize):
        filename = self.name + ".{:07d}".format(self.next_file_id) + self.extension
        objectStorage.createObject(filename, self.appendix[indStart:(indStart + blocksize)])
        self.files.append(filename)
        self.sizes.append(blocksize)
        self.first_locations.append(self.physicalsize)
        self.physicalsize += blocksize
        self.last_locations.append(self.physicalsize-1)
        self.next_file_id += 1
        
    def delete(self):
        self.appendix = bytearray()
        if self.physicalsize < 1:
            return
        objectStorage.deleteObject([self.index_name] + self.files)
        self.physicalsize    = 0
        self.files           = []
        self.sizes           = []
        self.first_locations = []
        self.last_locations  = []
        self.next_file_id    = 0

    def flushFiles(filenames, objectStorageFlush=False):
        for f in filenames:
            bigFile = VirtualBigFile(f,verboseOpen=1)
            if bigFile.physicalsize < 1:
                continue
            bigFile.flush(objectStorageFlush=objectStorageFlush)
            
    def deleteFiles(filenames):
        for f in filenames:
            bigFile = VirtualBigFile(f,verboseOpen=1)
            if bigFile.physicalsize < 1:
                continue
            bigFile.delete()
            pass
        pass
    
    pass

            
            