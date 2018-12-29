class DataBaseException(Exception):
    def __init__(self,messages):
        Exception.__init__(self)
        self.messages = messages