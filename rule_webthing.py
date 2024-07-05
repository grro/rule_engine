import tornado.ioloop
from webthing import (Property, Thing, Value)
from device import DeviceManager




class RuleThing(Thing):

    # regarding capabilities refer https://iot.mozilla.org/schemas
    # there is also another schema registry http://iotschema.org/docs/full.html not used by webthing

    def __init__(self, description: str, device_manager: DeviceManager):
        Thing.__init__(
            self,
            'urn:dev:ops:device_manager-1',
            'DeviceManager',
            ['MultiLevelSensor'],
            description
        )
        self.ioloop = tornado.ioloop.IOLoop.current()
        self.device_manager = device_manager
        self.device_manager.add_change_listener(self.on_value_changed)

        self.devices = Value(self.__devicenames)
        self.add_property(
            Property(self,
                     'devices',
                     self.devices,
                     metadata={
                         'title': 'devices',
                         "type": "sting",
                         'description': 'comma separated list of device names',
                         'readOnly': True,
                     }))

    @property
    def __devicenames(self) -> str:
        return ", ".join(sorted([device.name for device in self.device_manager.devices]))

    def on_value_changed(self):
        self.ioloop.add_callback(self._on_value_changed)

    def _on_value_changed(self):
        self.devices.notify_of_external_update(self.__devicenames)
