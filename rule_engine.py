import logging
import sys
import importlib
from device import DeviceManager
from rule_loader import RuleLoader
from source_scanner import parse_function_annotations
from loaded_rule_processor import RuleLoadedProcessor
from cron_processor import CronProcessor
from device import Store
from property_change_processor import PropertyChangeProcessor
from invoke import InvokerManager
from webthing import (MultipleThings, WebThingServer)
from db_webthing import StoreThing
from rule_webthing import RuleThing



class RuleEngine():

    def __init__(self, directory: str):
        self.__is_running = False
        self.__listener = lambda: None    # "empty" listener
        self.__directory = directory
        self.__invocation_manager = InvokerManager()
        self.__rule_loader = RuleLoader(self.__load_module, self.__unload_module, directory)
        self._device_manager = DeviceManager(directory)
        self._device_manager.add_change_listener(self.__rule_loader.reload)
        self.__processors = [RuleLoadedProcessor(self._device_manager, self.__invocation_manager),
                             CronProcessor(self._device_manager, self.__invocation_manager),
                             PropertyChangeProcessor(self._device_manager, self.__invocation_manager)]

    def set_listener(self, listener):
        self.__listener = listener

    def stop(self):
        self.__is_running = False
        self.__rule_loader.close()
        self._device_manager.close()

    def start(self):
        logging.info("starting rule engine...")
        self.__is_running = True
        if self.__directory not in sys.path:
            sys.path.insert(0, self.__directory )
        logging.info("starting invocation manager")
        self.__invocation_manager.start()
        logging.info("starting device_manager")
        self._device_manager.start()
        [processor.start() for processor in self.__processors]
        self.__rule_loader.start()
        logging.info("rule engine started")

    def __load_module(self, filename: str):
        if filename.endswith(".py"):
            try:
                modulename = self.__filename_to_modulename(filename)
                # reload?
                if modulename in sys.modules:
                    [processor.remove_rules(modulename) for processor in self.__processors]
                    importlib.reload(sys.modules[modulename])
                    msg = "file '" + filename + "' reloaded"
                else:
                    importlib.import_module(modulename)
                    msg = "file '" + filename + "' loaded"
                function_annotations = parse_function_annotations(modulename)
                if len(function_annotations) > 0:
                    [processor.on_annotations(function_annotations) for processor in self.__processors]
                    logging.info(msg)
                else:
                    logging.info("file '" + filename + "' ignored (no annotations)")
            except Exception as e:
                logging.warning("error occurred by (re)loading " + filename + " " + str(e), e)

    def __unload_module(self, filename: str, silent: bool = False):
        if filename.endswith(".py"):
            try:
                modulename = self.__filename_to_modulename(filename)
                if modulename in sys.modules:
                    [processor.remove_rules(modulename) for processor in self.__processors]
                    del sys.modules[modulename]
                    if not silent:
                        logging.info("'" + filename + "' unloaded")
            except Exception as e:
                logging.warning("error occurred by unloading " + filename + " " + str(e), e)

    def __filename_to_modulename(self, filename):
        return filename[:-3]



def run_webthing_server(description: str, port: int, device_manager: DeviceManager):
    server = WebThingServer(MultipleThings([RuleThing(description, device_manager), StoreThing(description, device_manager.device(Store.NAME))], "engine"), port=port, disable_host_validation=True)
    try:
        logging.info('starting the server http://localhost:' + str(port))
        server.start()
    except KeyboardInterrupt:
        logging.info('stopping the server')
        server.stop()
        logging.info('done')

def run_server(directory: str, port: int):
    rule_engine = RuleEngine(directory)
    try:
        logging.info('starting rule engine (rules dir: ' + directory + ')')
        rule_engine.start()
        run_webthing_server("", port, rule_engine._device_manager)

    except KeyboardInterrupt:
        logging.info('stopping rule engine')
        rule_engine.stop()
        logging.info('done')


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(name)-20s: %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('tornado.access').setLevel(logging.ERROR)
    run_server(sys.argv[1], int(sys.argv[2]))
