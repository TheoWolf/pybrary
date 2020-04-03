import os

def getFileName(filepath):
    return os.path.basename(filepath)


def getFileSize(filepath):
    return os.path.getsize(filepath)
