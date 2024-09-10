import logging
import requests
import json
import yaml
from os.path import join
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from redzoo.database.simple import SimpleDB
from abc import ABC, abstractmethod
from requests import Session
from datetime import datetime, timedelta
from threading import Thread
from time import sleep
from websocket_consumer import EventConsumer, Listener
from typing import Dict, Any, List, Optional




class Device(ABC):

    def __init__(self, name: str):
        self.name = name
        self.change_listeners = set()
        self._properties = {}

    def add_listener(self, change_listener):
        self.change_listeners.add(change_listener)

    def _notify_listener(self, props: Dict[str, Any]):
        for change_listener in self.change_listeners:
            change_listener(self, props)

    @property
    def property_names(self) -> List[str]:
        return list(self._properties.keys())

    def get_property(self, prop_name: str, dflt = None, force_loading: bool = False) -> Any:
        return self._properties.get(prop_name, dflt)

    def get_property_as_datetime(self, prop_name: str, dflt: datetime = None, timezone_offset: int = 0, force_loading: bool = False) -> datetime:
        dt_string = self.get_property(prop_name, dflt, force_loading)
        dt = datetime.strptime(dt_string, "%Y-%m-%dT%H:%M")
        dt = dt + timedelta(hours=timezone_offset)
        return dt

    @abstractmethod
    def set_property(self, name: str, value: Any, reason: str = None):
        pass

    def start(self):
        pass

    def close(self):
        pass

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


class Webthing(Device, Listener):

    def __init__(self, name: str, uri: str):
        super().__init__(name)
        if uri.endswith("/"):
            uri = uri[:-1]
        self.uri = uri
        self.__session = Session()
        self.__is_running = False
        self.__properties_load_time = dict()
        self.event_consumer = EventConsumer(name, self.uri, self).start()

    @staticmethod
    def create(name: str, uri: str) -> List:
        try:
            resp = requests.get(uri)
            resp.raise_for_status()
            data = resp.json()
            if type(data) is list:
                return [Webthing(config['title'], config['base']) for config in data]
            else:
                return [Webthing(name, uri)]
        except Exception as e:
            logging.warning(name + " error occurred calling " + uri + " " + str(e))
            return []


    def start(self):
        if not self.__is_running:
            self.__is_running = True
            try:
                self.__load_all_properties()
            except Exception as e:
                logging.warning(self.name + " error occurred loading properties " + str(e))
            Thread(target=self.__load_all_properties_loop, daemon=True).start()
            logging.info("device " + self.name + " started")

    def close(self):
        self.__is_running = False
        logging.info("disconnecting device " + self.name + " (" + self.uri + ")")
        self.event_consumer.stop()

    def on_property_changed(self, properties: Dict[str, Any]):
        props_changed = {}
        for name, value in properties.items():
            if name not in self._properties.keys() or value != self._properties[name]:
                props_changed[name] = value
            self._properties[name] = value
        self._notify_listener(props_changed)

    def get_property(self, prop_name: str, dlt = None, force_loading: bool = False):
        value = super().get_property(prop_name)

        if force_loading:
            loading = "force loading"
        elif value is None:
            loading = "local prop value is null"
        elif self.__property_age_sec(prop_name) > 180:
            loading = "local prop age is > 180 sec"
        else:
            loading = None

        if loading is not None:
            logging.debug("loading " + prop_name + ". Reason: " + loading)
            property_uri = self.uri + "/properties/" + prop_name
            try:
                resp = self.__session.get(property_uri, timeout=10)
                data = resp.json()
                value = data[prop_name]
                if value is None:
                    logging.warning("calling " + property_uri + " returns " + json.dumps(data, indent=2))
                self._properties[prop_name] = value
                self.__properties_load_time[prop_name] = datetime.now()
                self._notify_listener({prop_name: value})
            except Exception as e:
                logging.warning(self.name + " error occurred calling " + property_uri + " " + str(e))
                self.__renew_session()
        if value is None:
            return dlt
        else:
            return value

    def __property_age_sec(self ,prop_name: str) -> int:
        load_time = self.__properties_load_time.get(prop_name, datetime(year=2000, month=1, day=1))
        return int((datetime.now() - load_time).total_seconds())

    def set_property(self, prop_name: str, value: Any, reason: str = None):
        if self.get_property(prop_name, force_loading=True) != value:
            property_uri = self.uri + "/properties/" + prop_name
            try:
                data = json.dumps({prop_name: value})
                resp = self.__session.put(property_uri, data=data, timeout=10)
                if resp.status_code == 200:
                    self._properties[prop_name] = value
                    logging.info(self.uri + " updated: " + prop_name + "=" + str(value) + ("" if reason is None else " (" + reason + ")"))
                else:
                    logging.info(self.name + " calling " + self.uri + " to update " + prop_name + " with " + str(value) + " failed. Got " + str(resp.status_code) + " " + resp.text)
                self._notify_listener({prop_name: value})
            except Exception as e:
                logging.warning(self.name + " error occurred calling " + property_uri + " " + str(e))
                self.__renew_session()

    def __load_all_properties(self):
        property_uri = self.uri + "/properties"
        try:
            resp = self.__session.get(property_uri, timeout=10)
            if resp.status_code == 200:
                props = resp.json()
                self._properties.update(props)
                self._notify_listener(props)
            else:
                logging.warning(self.name + " got error response calling " + property_uri + " " + str(resp.status_code) + " " + resp.text)
        except Exception as e:
            logging.warning(self.name + " error occurred calling " + property_uri + " " + str(e))
            self.__renew_session()

    def __load_all_properties_loop(self):
        while self.__is_running:
            self.__load_all_properties()
            sleep(13 * 60)

    def __renew_session(self):
        logging.info(self.name + " renew session")
        try:
            self.__session.close()
        except Exception as e:
            logging.warning(str(e))
        self.__session = Session()

    def __hash__(self):
        return hash(self.name + self.uri)

    def __eq__(self, other):
        return self.name == other.name and self.uri == other.uri

    def __str__(self):
        return self.name + " (" + self.uri + ")"


class Store(Device):

    NAME = "db"

    def __init__(self, directory: str, name: str = "rule_db"):
        super().__init__(self.NAME)
        self.__listener =  lambda: None
        self.__db = SimpleDB(name, directory=directory)

    def set_listener(self, listener):
        self.__listener = listener

    @property
    def property_names(self) -> List[str]:
        return  self.__db.keys()

    def get_property(self, prop_name: str, dlt = None, force_loading: bool = False):
        return self.__db.get(prop_name, default_value=dlt)

    def set_property(self, prop_name: str, value: Any, reason: str = None):
        self.__db.put(prop_name, value)
        logging.info("update:" + prop_name + "=" + str(value) + ("" if reason is None else " (" + reason + ")"))
        self.__listener(prop_name)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return self.name



class DeviceRegistry(ABC):

    @abstractmethod
    def device(self, name: str) -> Optional[Device]:
        pass

    @abstractmethod
    def devices(self) -> List[Device]:
        pass


class DeviceManager(DeviceRegistry, FileSystemEventHandler):

    FILENAME = "webthings.yml"

    def __init__(self, dir: str):
        self.__is_running = True
        self.dir =  dir
        self.__change_listeners = set()
        self.__db_device = Store(join(dir, 'data'))
        self.__device_map = { self.__db_device.name: self.__db_device }
        self.observer = Observer()
        self.__last_time_reloaded = datetime.now() - timedelta(days=300)

    def add_change_listener(self, change_listener):
        self.__change_listeners.add(change_listener)

    def start(self):
        self.observer.schedule(self, self.dir, recursive=False)
        self.observer.start()
        self.__reload_config()

    def close(self):
        self.__is_running = False
        self.observer.stop()
        for device in self.__device_map.values():
            device.close()

    @property
    def devices(self) -> List[Device]:
        return list(self.__device_map.values())

    def device(self, name: str) -> Optional[Device]:
        device = self.__device_map.get(name, None)
        if device is None:
            elapsed_sec = (datetime.now() - self.__last_time_reloaded).total_seconds()
            max_frequency = 30
            if elapsed_sec > max_frequency:
                logging.warning("device " + name + " not available. Reloading config")
                self.__reload_config()
                device = self.__device_map.get(name, None)
            else:
                logging.warning("device " + name + " not available. Suppress reloading config (was tried " + str(int(elapsed_sec)) + " sec ago; min wait time: " + str(max_frequency) + " sec)")
        if device is None:
            logging.warning("device " + name + " not available. Returning None (available devices: " + ", " .join([device.name for device in self.devices]) + ")")
        return device

    def dispatch(self, event: FileSystemEvent) -> None:
        super().dispatch(event)

    def on_moved(self, event):
        if event.dest_path.endswith(self.FILENAME):
            self.__reload_config()

    def on_deleted(self, event):
        if event.src_path.endswith(self.FILENAME):
            self.__reload_config()

    def on_created(self, event):
        if event.src_path.endswith(self.FILENAME):
            self.__reload_config()

    def on_modified(self, event):
        if event.src_path.endswith(self.FILENAME):
            self.__reload_config()

    def __notify_listeners(self):
        try:
            [change_listener() for change_listener in self.__change_listeners]
        except Exception as e:
            logging.warning("error occurred notifying listeners " + str(e))

    def __reload_config(self):
        if self.__is_running:
            self.__last_time_reloaded = datetime.now()
            try:
                webthing_file = join(self.dir, self.FILENAME)
                logging.info("reading " + webthing_file)
                with open(webthing_file) as file:
                    for device_name, config in yaml.safe_load(file).items():
                        for device in Webthing.create(device_name, config['url']):
                            if device.name not in self.__device_map.keys():
                                device.start()
                                self.__device_map[device.name] = device
                logging.info("devices available: " + ", ".join(sorted([device.name for device in self.devices])))
            except Exception as e:
                logging.warning("error occurred refreshing config " + str(e))
            self.__notify_listeners()
        else:
            [device.close() for device in self.__device_map.values()]

