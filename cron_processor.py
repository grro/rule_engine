import logging
import pycron
from time import sleep
from threading import Thread
from datetime import datetime
from rule import Rule
from device import DeviceRegistry
from invoke import InvokerManager
from processor import Processor




class CronRule(Rule):

    def __init__(self, trigger_expression: str, cron: str, func, invoker_manager: InvokerManager):
        self.cron = cron
        super().__init__(trigger_expression, func, invoker_manager)


class CronProcessor(Processor):

    def __init__(self, device_registry: DeviceRegistry, invoker_manager: InvokerManager):
        self.thread = Thread(target=self.__process, daemon=True)
        self.last_execution = datetime.fromtimestamp(0)
        super().__init__("cron", device_registry, invoker_manager)

    def on_annotation(self, annotation: str, func) -> bool:
        if annotation.lower().startswith("time cron"):
            cron = annotation[len("time cron"):].strip()
            if self.is_vaild_cron(cron):
                self.add_rule(CronRule(annotation, cron, func, self._invoker_manager))
                return True
            else:
                logging.warning("cron " + cron + " is invalid (syntax error?)")
        return False

    def is_vaild_cron(self, cron: str) -> bool:
        try:
            pycron.is_now(cron)
            return True
        except Exception as e:
            return False

    def __process(self):
        while self.is_running:
            try:
                if (datetime.now() - self.last_execution).total_seconds() >= 60:  # minimum 60 sec!
                    self.last_execution = datetime.now()
                    for rule in list(self.rules):
                        if pycron.is_now(rule.cron):
                            self.invoke_rule(rule)
            except Exception as e:
                logging.warning("Error occurred by executing cron", e)
            sleep(5)

    def on_start(self):
        self.thread.start()

