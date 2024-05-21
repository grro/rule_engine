import logging
from datetime import datetime
from invoke import InvokerManager
from device import DeviceRegistry


class Rule:

    def __init__(self, trigger_expression: str, func, invoker_manager: InvokerManager):
        self.trigger_expression = trigger_expression
        self.__func = func
        self.__invoker = invoker_manager.new_invoker(func)
        self.last_executed = None
        self.last_failed = None

    def invoke(self, device_registry: DeviceRegistry, initiator: str):
        try:
            logging.debug('executing ' + self.module + '.py#' + self.function_name + '(...) on @when("' + self.trigger_expression + '")')
            self.__invoker.invoke(device_registry, initiator)
            self.last_executed = datetime.now()
        except Exception as e:
            logging.warning("Error occurred by executing rule " + self.function_name, e)
            self.last_failed = datetime.now()

    @property
    def module(self) -> str:
        return self.__func.__module__

    @property
    def function_name(self) -> str:
        return self.__func.__name__

    def fingerprint(self) -> str:
        return str(self.module) + "/" + str(self.function_name) + "/" + self.trigger_expression

    def __hash__(self):
        return hash(self.fingerprint())

    def __eq__(self, other):
        return self.fingerprint() == other.fingerprint()

    def __lt__(self, other):
        return self.fingerprint() < other.fingerprint()

