import json

class Task(object):
    def __init__(self,data=None,urn=None):
        self._data = json.dumps(data)
        self.urn = urn
        
    @property
    def data(self):
        return json.loads(self._data) 
    
    @property
    def __repr__(self):
        return '%s(%s)' %(self.__class__.__name__,repr(self.data))