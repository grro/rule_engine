import logging
import requests
import json
from os.path import join
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dateutil import tz
import yaml
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

    def get_property_as_datetime(self, prop_name: str, dflt: datetime = None, force_loading: bool = False) -> datetime:
        dt = self.get_property(prop_name, dflt, force_loading)
        return datetime.strptime(dt, "%Y-%m-%dT%H:%M")

    def get_property_as_local_datetime(self, prop_name: str, dflt: datetime = None, force_loading: bool = False) -> datetime:
        dt = self.get_property_as_datetime(prop_name, dflt, force_loading)
        return self.__utc_to_local_time(dt)

    def __utc_to_local_time(self, utc: datetime) -> datetime:
        from_zone = tz.tzutc()
        to_zone = tz.tzlocal()
        utc = utc.replace(tzinfo=from_zone)
        return utc.astimezone(to_zone)


    @abstractmethod
    def set_property(self, name: str, value: Any, reason: str = None):
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
        if force_loading or (value is None):
            property_uri = self.uri + "/properties/" + prop_name
            try:
                resp = self.__session.get(property_uri, timeout=10)
                data = resp.json()
                value = data[prop_name]
                if value is None:
                    logging.warning("calling " + property_uri + " returns " + json.dumps(data, indent=2))
                self._properties[prop_name] = value
                self._notify_listener({prop_name: value})
            except Exception as e:
                logging.warning(self.name + " error occurred calling " + property_uri + " " + str(e))
                self.__renew_session()
        if value is None:
            return dlt
        else:
            return value

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



class DeviceRegistry(ABC):

    @abstractmethod
    def device(self, name: str) -> Optional[Device]:
        pass

    @abstractmethod
    def devices(self) -> List[Device]:
        pass


class DeviceManager(DeviceRegistry, FileSystemEventHandler):

    FILENAME = "webthings.yml"

    def __init__(self, dir: str, change_listener):
        self.__is_running = True
        self.dir =  dir
        self.__change_listener = change_listener
        self.__device_map = {}
        self.observer = Observer()
        self.__last_time_reloaded = datetime.now() - timedelta(days=300)

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
            if datetime.now() > (self.__last_time_reloaded + timedelta(minutes=10)):
                self.__reload_config()
                device = self.__device_map.get(name, None)
        if device is None:
            logging.warning("device " + name + " not available. Returning None (available devices: " + ", " .join([device.name for device in self.devices]) + ")")
        return device

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
            self.__change_listener()
        else:
            [device.close() for device in self.__device_map.values()]

