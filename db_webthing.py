import tornado.ioloop
from webthing import (SingleThing, Property, Thing, Value, WebThingServer)
from device import Store, DeviceManager





class StoreThing(Thing):

    # regarding capabilities refer https://iot.mozilla.org/schemas
    # there is also another schema registry http://iotschema.org/docs/full.html not used by webthing

    def __init__(self, description: str, store: Store):
        Thing.__init__(
            self,
            'urn:dev:ops:db-1',
            'DB',
            ['MultiLevelSensor'],
            description
        )
        self.ioloop = tornado.ioloop.IOLoop.current()
        self.store = store
        self.store.set_listener(self.on_value_changed)

        self.__props = {}

        for name in self.store.property_names:
            val = store.get_property(name)
            prop = Value(val, lambda value: self.store.set_property(name, value))
            dt = "sting"
            if type(val) == bool:
                dt = "boolean"
            elif type(val) == float:
                dt = "number"
            self.add_property(Property(self,
                                       name,
                                       prop,
                                       metadata={
                                           'title': name,
                                           "type": dt,
                                           'readOnly': False,
                                       }))
            self.__props[name] = prop

    def on_value_changed(self, name: str):
        self.ioloop.add_callback(lambda: self._on_value_changed(name))

    def _on_value_changed(self, name):
        val = self.__props.get(name, None)
        if val is not None:
            val.notify_of_external_update(self.store.get_property(name))



# test curl
# curl -X PUT -d '{"time": 5.7}' http://localhost:9966/properties/time