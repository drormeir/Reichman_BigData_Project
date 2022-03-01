import os
import threading
import atexit

# implementing a thread safe LRU cache with dictionary

class MockObjectStorage:
    def __init__(self, MaxCachedFiles=32):
        self.MaxCachedFiles = MaxCachedFiles
        if MaxCachedFiles > 0:
            self.cache  = {}
            self.oldest = None
            self.newest = None
            self.lock   = threading.Lock()
            atexit.register(self.flush)
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.MaxCachedFiles > 0:
            self.flush()
        
    def createObject(self, names, datas):
        if self.MaxCachedFiles > 0:
            self.lock.acquire()
            self.__appendData2Cache__(names,datas,True)
            self.lock.release()
        else:
            self.__writeData__(names,datas)

    def readObject(self, names, type_=None):
        islist = isinstance(names,list)
        if not islist:
            names = [names]
        res = []
        if self.MaxCachedFiles > 0:
            self.lock.acquire()
        for name in names:
            if self.MaxCachedFiles < 1 or name not in self.cache:
                # new object does not exists in cache --> read it and append into cache
                if not os.path.isfile(name):
                    res.append(None)
                    continue
                with open(name,"rb") as f:
                    data = f.read()
                if self.MaxCachedFiles > 0:
                    self.__appendData2Cache__(name,data,False)
                res.append(data)
                continue
            # new object already exists in cache
            data_node     = self.cache[name]
            if self.newest != name:
                # this node is not newest --> cache needs to be updated
                old_data_next = data_node[2]
                old_data_prev = data_node[3]
                data_node[2]  = None
                data_node[3]  = self.newest
                self.cache[self.newest][2] = name
                self.newest   = name
                self.cache[old_data_next][3] = old_data_prev
                if self.oldest == name:
                    self.oldest = old_data_next
                else:
                    self.cache[old_data_prev][2] = old_data_next
            res.append(data_node[0])
        if self.MaxCachedFiles > 0:
            self.lock.release()
        if type_ is not None:
            # convert all bytes arrays into strings
            res = [MockObjectStorage.convertBytes2Str(r,type_) if isinstance(r, bytes) else r for r in res]
        return res if islist or len(res) > 1 else res[0]
    
    def deleteObject(self, names):
        if not isinstance(names,list):
            names = [names]
        if self.MaxCachedFiles < 1:
            for name in names:
                if os.path.exists(name):
                    os.remove(name)
            return
        self.lock.acquire()
        for name in names:
            deleteFromDisk = True
            if name in self.cache:
                data_node = self.cache[name]
                old_data_next = data_node[2]
                old_data_prev = data_node[3]
                if self.oldest == name:
                    self.oldest = old_data_next
                else:
                    self.cache[old_data_prev][2] = old_data_next
                if self.newest == name:
                    self.newest = old_data_prev
                else:
                    self.cache[old_data_next][3] = old_data_prev
                deleteFromDisk = not data_node[1]
                del self.cache[name]
            if deleteFromDisk and os.path.exists(name):
                os.remove(name)
        self.lock.release()
    
    def flush(self, names=None):
        if self.MaxCachedFiles < 1:
            return
        self.lock.acquire()
        for key in self.cache:
            node = self.cache[key]
            if not node[1]:
                continue
            if names is not None and key not in names:
                continue
            self.__writeData__(key,node[0])
            node[1] = False
        self.lock.release()
        
    def __appendData2Cache__(self, names, datas, status):
        if isinstance(names,list):
            assert isinstance(datas,list)
            assert len(datas) == len(names)
            is_str = isinstance(datas[0],str) or isinstance(datas[0],list)
        else:
            is_str = isinstance(datas,str) or isinstance(datas,list)
            assert not is_str or isinstance(datas,str) or isinstance(datas,list) and isinstance(datas[0],str)
            names  = [names]
            datas  = [datas]
        for name,data in zip(names,datas):
            assert name not in self.cache
            if is_str:
                data = MockObjectStorage.convertStr2Bytes(data)
            prev_newest = self.newest
            self.cache[name] = [data,status,None,prev_newest]
            self.newest = name
            if self.oldest is None:
                self.oldest = name
                return
            if prev_newest is not None:
                self.cache[prev_newest][2] = name
            if len(self.cache) <= self.MaxCachedFiles:
                return
            # removing oldest data from cache
            prev_oldest_name = self.oldest
            prev_oldest_node = self.cache[prev_oldest_name]
            self.oldest = prev_oldest_node[2]
            self.cache[self.oldest][3] = None
            if prev_oldest_node[1]:
                self.__writeData__(prev_oldest_name,prev_oldest_node[0])
            del self.cache[prev_oldest_name]
        
    def __writeData__(self,names,datas):
        if isinstance(names,list):
            assert isinstance(datas,list)
            assert len(datas) == len(names)
            is_str = isinstance(datas[0],str) or isinstance(datas[0],list)
        else:
            is_str = isinstance(datas,str) or isinstance(datas,list)
            assert not is_str or isinstance(datas,str) or isinstance(datas,list) and isinstance(datas[0],str)
            names  = [names]
            datas  = [datas]
        for name,data in zip(names,datas):
            if len(data) < 1:
                continue
            if is_str:
                data = MockObjectStorage.convertStr2Bytes(data)
            with open(name,"wb") as f:
                f.write(data)
        return
        
    def convertStr2Bytes(str_):
        # joining all strings into one big string
        if isinstance(str_,str):
            datarows = str_
        elif isinstance(str_,list):
            if isinstance(str_[0],str):
                datarows = '\n'.join(str_)
            elif isinstance(str_[0],tuple) and isinstance(str_[0][0],str):
                datarows = '\n'.join([','.join(tup) for tup in str_])
        # make sure big string ends with end-of-line before converting to bytes
        datarows = datarows if datarows[-1]=='\n' else datarows + '\n'
        return datarows.encode('ASCII')
    
    def convertBytes2Str(bytes_, type_=str):
        res     = bytes_.decode('ASCII').replace('\r\n','\n')
        is_list = isinstance(type_,list)
        if not is_list:
            assert type_ == str
            return res
        res = res.split('\n')
        if bytes_[-1] == ord('\n'):
            # removing last empty string
            del res[-1]
        if type_[0] == str:
            return res
        if type_[0] == tuple:
            return [tuple(row.split(',')) for row in res]
        assert False
