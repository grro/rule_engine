import logging
import json
import requests
from websocket import create_connection
from abc import ABC, abstractmethod
from typing import Any, Dict
from threading import Thread
from time import sleep



class Listener(ABC):

    @abstractmethod
    def on_property_changed(self, properties: Dict[str, Any]):
        pass



class EventConsumer:

    def __init__(self, device: str, uri: str, event_listener: Listener):
        self.__is_running = True
        self.__uri = uri
        self.device = device
        self.__ws_uri = None
        self.__event_listener = event_listener

    @property
    def ws_uri(self) -> str:
        if self.__ws_uri is None:
            resp = requests.get(self.__uri)
            data = resp.json()
            for link in data['links']:
                if link['href'].startswith("ws"):
                    self.__ws_uri = link['href']
                    break
        return self.__ws_uri

    def start(self):
        Thread(target=self.__listen, daemon=True).start()
        return self

    def stop(self):
        self.__is_running = False

    def on_message(self, message: str):
        try:
            data = json.loads(message)
            if data['messageType'] == 'propertyStatus':
                self.__event_listener.on_property_changed(data['data'])
            else:
                logging.warning("device " + self.device + " unknown message type received " + message)
        except Exception as e:
            logging.warning("device " + self.device + " error occurred parsing message " + message + " " + str(e))

    def __listen(self):
        errors = 0
        while self.__is_running:
            ws = None
            try:
                logging.info("device " + self.device + " opening stream " + self.ws_uri)
                ws = create_connection(self.ws_uri)
                while self.__is_running:
                    msg = ws.recv()
                    self.on_message(msg)
                    errors = 0
            except Exception as e:
                errors = errors + 1
                if self.__is_running:
                    logging.warning("device " + self.device + " error occurred running websocket client (" + self.__uri + ") " + str(e))
            try:
                ws.close()
            except Exception as e:
                pass

            if errors < 3:
                sleep(3)
            elif errors < 5:
                sleep(30)
            else:
                sleep(5*60)
