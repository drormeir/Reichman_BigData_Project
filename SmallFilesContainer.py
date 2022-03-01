from VirtualBigFile import *
import threading
import atexit

class SmallFilesContainer:
    def __init__(self, name = "SmallFiles.bin", blocksize=0):
        self.virtualBigFile = VirtualBigFile(name=name, blocksize=blocksize)
        self.files          = {}
        num_partitions      = self.virtualBigFile.num_partitions()
        if num_partitions > 0:
            indexes = self.virtualBigFile.readPartition(num_partitions-1)
            indexes = MockObjectStorage.convertBytes2Str(indexes,[tuple])
            for row in indexes:
                self.files[row[0]] = [int(col) for col in row[1:]]
        self.new_index      = False
        self.last_created   = ""
        self.lock           = threading.Lock()
        atexit.register(self.flush)
        return
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()
        return
    
    def flush(self, objectStorageFlush=False, useLock=True):
        if useLock:
            self.lock.acquire()
        if self.new_index:
            if len(self.files) < 1:
                self.virtualBigFile.delete()
            else:
                self.virtualBigFile.writeAppendix()
                indexes = [','.join([key] + [str(v) for v in values]) for key,values in self.files.items()]
                self.virtualBigFile.append(indexes)            
            self.virtualBigFile.flush(objectStorageFlush)
            self.new_index      = False
            self.last_created   = ""
        if useLock:
            self.lock.release()
        return
        
    def readFile(self,name,type_=None, useLock=True):
        if useLock:
            self.lock.acquire()
        ret = None
        while name in self.files:
            file_rec      = self.files[name]
            ind_partition = file_rec[0]
            if ind_partition == self.virtualBigFile.num_partitions():
                partition = self.virtualBigFile.appendix
            else:
                partition = self.virtualBigFile.readPartition(ind_partition)
            file_data     = partition[file_rec[1]:file_rec[2]]
            if type_ is None:
                type_    = file_rec[3]
            if isinstance(type_,bool) or isinstance(type_,int) and abs(type_) <= 1:
                if not type_:
                    ret = file_data
                    break
                type_ = [str]
            ret = MockObjectStorage.convertBytes2Str(file_data,type_)
            break
        if useLock:
            self.lock.release()
        return ret    
    
    def appendData(self, name, data, useLock=True):
        if useLock:
            self.lock.acquire()
        if name in self.files:
            self.appendExistingFile(name=name,data=data,useLock=False)
        else:
            self.createNewFile(name=name,data=data,useLock=False)
        if useLock:
            self.lock.release()
        return
    
    def appendExistingFile(self, name, data, useLock=True):
        if useLock:
            self.lock.acquire()
        while True:
            assert name in self.files
            file_rec          = self.files[name]
            existing_str_type = file_rec[3]
            if existing_str_type:
                data = MockObjectStorage.convertStr2Bytes(data)
            if name == self.last_created:
                # try to append new data into existing appendix without writing into the disk
                length       = len(data)
                appendix     = self.virtualBigFile.appendix
                len_appendix = len(appendix)
                if len_appendix + length <= self.virtualBigFile.blocksize:
                    self.virtualBigFile.append(data)
                    file_rec[2] += length
                    break
                old_start, old_end = file_rec[1], file_rec[2]
                old_data = appendix[old_start:old_end]
                self.virtualBigFile.appendix = appendix[:old_start]
            else:
                old_data = self.readFile(name,is_str=False)
            self.createNewFile(name,old_data + data,existing_str_type,useLock=False)
            break
        if useLock:
            self.lock.release()
        return

    def createNewFile(self,name,data,existing_str_type=None, deleteExist=False, useLock=True):
        if useLock:
            self.lock.acquire()
        if deleteExist and name in self.files:
            del self.files[name]
        assert (name in self.files) != (existing_str_type is None)
        if existing_str_type is not None:
            is_str = existing_str_type
        else:
            self.last_created = name
            is_str = int(isinstance(data,list) or isinstance(data,str))
            if is_str:
                data = MockObjectStorage.convertStr2Bytes(data)
        length           = len(data)
        assert length <= self.virtualBigFile.blocksize
        len_appendix     = len(self.virtualBigFile.appendix)
        num_partitions   = self.virtualBigFile.num_partitions() # nicluding appendix partition
        if len_appendix + length > self.virtualBigFile.blocksize:
            self.virtualBigFile.writeAppendix()
            ind_partition = num_partitions
            ind_start     = 0
        else:
            ind_partition = num_partitions - int(len_appendix > 0)
            ind_start     = len_appendix
        self.virtualBigFile.append(data)
        self.files[name] = [ind_partition,ind_start,ind_start + length,is_str]
        self.new_index   = True
        if useLock:
            self.lock.release()
        return
    
    def deleteAllFiles(self, useLock=True):
        if useLock:
            self.lock.acquire()
        self.files        = {}
        self.last_created = ""
        self.virtualBigFile.delete()
        self.new_index = True
        if useLock:
            self.lock.release()
        return

    def deleteFiles(self,names,mustExist=False, useLock=True):
        if useLock:
            self.lock.acquire()
        ret = True
        while True:
            if not isinstance(names,list):
                names = [names]
            for name in names:
                if name not in self.files:
                    if not mustExist:
                        continue
                    ret = False
                    break
                del self.files[name]
                self.new_index = True
            if len(self.files) < 1:
                self.deleteAllFiles(useLock=False)
            break
        if useLock:
            self.lock.release()
        return ret
    
    def getFileNames(self, useLock=True):
        if useLock:
            self.lock.acquire()
        ret = list(self.files.keys())
        if useLock:
            self.lock.release()
        return ret
    
