from rule import Rule
from invoke import InvokerManager
from processor import Processor
from device import DeviceRegistry


class RuleLoadedProcessor(Processor):

    def __init__(self, device_registry: DeviceRegistry, invoker_manager: InvokerManager):
        super().__init__("rule loaded", device_registry, invoker_manager)

    def on_annotation(self, annotation: str, func):
        if annotation.lower().strip() == "rule loaded":
            self.add_rule(Rule(annotation, func, self._invoker_manager))
            return True
        return False

    def on_add_rule(self, rule: Rule):
        self.invoke_rule(rule)
