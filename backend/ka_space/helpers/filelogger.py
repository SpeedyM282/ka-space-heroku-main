import os


class FileLogger:
    def __init__(self, file):
        self.file = file

    def append(self, data):
        with open(self.file, "a") as f:
            f.write(f"{data}\n")
