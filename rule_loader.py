import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler



class RuleLoader(FileSystemEventHandler):

    def __init__(self, load_listener, unload_listener, dir):
        self.load_listener = load_listener
        self.unload_listener = unload_listener
        self.dir = dir
        self.observer = Observer()

    def __unload_module(self, path: str):
        self.unload_listener(path)

    def __load_module(self, path: str):
        try:
            self.load_listener(path)
        except Exception as e:
            logging.error(e)

    def start(self):
        try:
            logging.info("observing rules directory '" + self.dir + "' started")
            self.reload()
            self.observer.schedule(self, self.dir, recursive=False)
            self.observer.start()
        except Exception as e:
            logging.error("error occurred starting file listener " + str(e))

    def reload(self):
        try:
            files = [file for file in os.scandir(self.dir) if file.name.endswith(".py")]
            logging.debug(str(len(files)) + " files found: " + ", ".join([file.name for file in files]))
            for file in files:
                self.__load_module(file.name)
        except Exception as e:
            logging.error("error occurred starting file listener " + str(e))

    def close(self):
        self.observer.stop()
        logging.info("observing rules directory '" + self.dir + "' stopped")

    def on_moved(self, event):
        self.__unload_module(self.filename(event.src_path))
        self.__load_module(self.filename(event.dest_path))

    def on_deleted(self, event):
        logging.debug("file " + self.filename(event.src_path) + " deleted")
        self.__unload_module(self.filename(event.src_path))

    def on_created(self, event):
        self.__load_module(self.filename(event.src_path))

    def on_modified(self, event):
        logging.debug("file " + self.filename(event.src_path) + " modified")
        self.__unload_module(self.filename(event.src_path))
        self.__load_module(self.filename(event.src_path))

    def filename(self, path):
        path = path.replace("\\", "/")
        return path[path.rindex("/")+1:]

