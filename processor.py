import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any
from rule import Rule
from device import DeviceRegistry
from invoke import InvokerManager


class Processor(ABC):

    def __init__(self, name: str, device_registry: DeviceRegistry, invoker_manager: InvokerManager):
        self.name = name
        self._device_registry = device_registry
        self._invoker_manager = invoker_manager
        self.is_running = False
        self.rules = set()

    def on_annotations(self, function_annotations: Dict[Any, List[str]]):
        for func, annotations in function_annotations.items():
            for annotation in annotations:
                self.on_annotation(annotation, func)

    @abstractmethod
    def on_annotation(self, annotation: str, func) -> bool:
        pass

    def add_rule(self, rule: Rule):
        logging.info(' * register ' + rule.module + '.py#' + rule.function_name + '(...) on @when("' + rule.trigger_expression + '")')
        self.rules.add(rule)
        self.on_add_rule(rule)

    def remove_rules(self, module: str):
        rules_of_module = {rule for rule in self.rules if rule.module == module}
        for rule in rules_of_module:
            logging.info(' * unregister ' + rule.module + '.py#' + rule.function_name + '(...) on @when("' + rule.trigger_expression + '")')
        self.rules = self.rules - rules_of_module
        self.on_remove_rules(module)

    def invoke_rule(self, rule: Rule):
        try:
            rule.invoke(self._device_registry)
        except Exception as e:
            logging.warning("Error occurred by executing rule " + rule.function_name, e)

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.on_start()
            logging.info("'" + self.name + " processor' started")

    def on_start(self):
        pass

    def stop(self):
        self.is_running = False
        self.on_stop()
        logging.info("'" + self.name + "' processor stopped")

    def on_stop(self):
        pass

    def on_add_rule(self, rule: Rule):
        pass

    def on_remove_rules(self, module: str):
        pass

