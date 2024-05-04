from rule import Rule
from typing import Dict, Any
from invoke import InvokerManager
from processor import Processor
from device import DeviceRegistry, Device



class PropertyChangedRule(Rule):

    def __init__(self, device_name: str, property_name: str, trigger_expression: str, func, invoker_manager: InvokerManager):
        self.property_name = property_name
        self.device_name = device_name
        super().__init__(trigger_expression, func, invoker_manager)

    def matches(self, device_name: str, property_name: str) -> bool:
        return self.device_name == device_name and self.property_name == property_name


class PropertyChangeProcessor(Processor):

    def __init__(self, device_registry: DeviceRegistry, invoker_manager: InvokerManager):
        super().__init__("Property change", device_registry, invoker_manager)

    def on_annotation(self, annotation: str, func) -> bool:
        if annotation.lower().startswith("property") and annotation.lower().endswith("changed"):
            device_property_pair = annotation[len("property"):len("changed") *-1].strip()
            device, property = device_property_pair.split("#")
            self._device_registry.device(device).add_listener(self.__on_property_changed)
            self.add_rule(PropertyChangedRule(device, property, annotation, func, self._invoker_manager))
            return True
        return False

    def __on_property_changed(self, device: Device, properties: Dict[str, Any]):
        for name, value in properties.items():
            for changed_rule in [rule for rule in self.rules if rule.matches(device.name, name)]:
                self.invoke_rule(changed_rule)

