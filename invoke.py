import inspect
import logging
from abc import ABC, abstractmethod
from typing import Optional, List
from queue import Queue, Empty
from datetime import datetime
from threading import Thread, Lock
from device import DeviceRegistry



class Invoker(ABC):

    @abstractmethod
    def invoke(self, device_registry: DeviceRegistry, initiator: str):
        pass



class InvokerImpl(Invoker):

    TYPE_SINGLE_PARAM_ITEMREGISTRY = "TYPE_SINGLE_PARAM_ITEMREGISTRY"

    @staticmethod
    def create(func) -> Optional:
        type = ""
        spec = inspect.getfullargspec(func)

        # one argument ItemRegistry
        if len(spec.args) == 1:
            type = InvokerImpl.TYPE_SINGLE_PARAM_ITEMREGISTRY
            if spec.args[0] in spec.annotations:
                if spec.annotations[spec.args[0]] != DeviceRegistry:
                    logging.warning("parameter " + str(spec.args[0]) + " is of type " + str(spec.annotations[spec.args[0]]) + ". " +
                                    str(spec.annotations[spec.args[0]]) + " is not supported (supported: DeviceRegistry)")
                    return None
            else:
                logging.warning("assuming that parameter " + spec.args[0] + " is of type DeviceRegistry. " \
                                                                            "Please use type hints such as " + func.__name__ + "(" + spec.args[0]  + ": DeviceRegistry)")
        return InvokerImpl(func, type)

    def __init__(self, func, type: str):
        self._func = func
        self.name = func.__name__
        self.fullname = func.__module__ + "#" + self.name
        self.__type = type

    def __str__(self):
        return self.fullname

    def invoke(self, device_registry: DeviceRegistry, initiator: str):
        try:
            logging.debug("calling " + str(self._func.__name__) + " (initiator: " + initiator + ")")
            if self.__type == self.TYPE_SINGLE_PARAM_ITEMREGISTRY:
                self._func(device_registry)
            else:
                self._func()
        except Exception as e:
            raise Exception("Error occurred executing function " + self.fullname + "(...)" + " " + str(e)) from e


class AsyncInvokerWrapper(Invoker):

    @staticmethod
    def create(invoker: Invoker, invoker_manager) -> Optional:
        if invoker is None:
            return None
        else:
            return AsyncInvokerWrapper(invoker, invoker_manager)

    def __init__(self, invoker: Invoker, invoker_manager):
        self.invoker = invoker
        self.invoker_manager = invoker_manager

    def invoke(self, device_registry: DeviceRegistry, initiator: str):
        self.invoker_manager.invoke_async(Invocation(self.invoker, device_registry, initiator))


class Invocation:

    def __init__(self, invoker: Invoker, device_registry : DeviceRegistry, initiator: str):
        self.invoker = invoker
        self.initiator = initiator
        self.device_registry = device_registry

    def invoke(self):
        self.invoker.invoke(self.device_registry, self.initiator)

    def __str__(self):
        return str(self.invoker)

class InvokerManager:

    def __init__(self, num_runners: int = 10):
        self.is_running = True
        self.num_runners = num_runners
        self.__listeners = set()
        self.__lock = Lock()
        self.__running_invocations = {}
        self.__queue = Queue()

    def running_invocations(self) -> List[str]:
        with self.__lock:
            info = []
            for invoker, running_since in self.__running_invocations.items():
                info.append(str(invoker) + " (since " + str((datetime.now() - running_since)) + ")")
            return sorted(info)

    def add_listener(self, listener):
        self.__listeners.add(listener)
        self.__notify_listener()

    def __notify_listener(self):
        for listener in self.__listeners:
            try:
                listener()
            except Exception as e:
                logging.warning("error occurred calling " + str(listener) + " " + str(e))

    def start(self):
        [Thread(target=self.process_invoke_runner, daemon=True, args=(i,)).start() for i in range(0, self.num_runners)]

    def stop(self):
        self.is_running = False

    def register_running(self, invocation_runner : Invocation) -> Optional[datetime]:
        try:
            with self.__lock:
                if invocation_runner.invoker in self.__running_invocations.keys():
                    return self.__running_invocations[invocation_runner.invoker]
                else:
                    self.__running_invocations[invocation_runner.invoker] = datetime.now()
                    return None
        finally:
            self.__notify_listener()

    def deregister_running(self, invocation_runner : Invocation):
        try:
            with self.__lock:
                self.__running_invocations.pop(invocation_runner.invoker, None)
        finally:
            self.__notify_listener()


    def invoke_async(self, invocation: Invocation):
        self.__queue.put(invocation)

    def process_invoke_runner(self, runner_id: int):
        while self.is_running:
            try:
                invocation = self.__queue.get(timeout=3)
                running_since = self.register_running(invocation)
                if running_since is None:
                    try:
                        logging.debug("[runner" + str(runner_id) + "] invoking " + str(invocation))
                        invocation.invoke()
                    except Exception as e:
                        logging.warning("[runner" + str(runner_id) + "] error occurred calling " + str(invocation) + " " + str(e), e)
                    finally:
                        self.deregister_running(invocation)
                else:
                    elapsed = datetime.now() - running_since
                    if elapsed.total_seconds() > 2 * 60:
                        logging.warning("[runner" + str(runner_id) + "] reject invoking " + str(invocation) + " Invocation hangs (since " + str(elapsed) + ")")
                    else:
                        logging.debug("[runner" + str(runner_id) + "] reject invoking " + str(invocation) + " Invocation is already running (since " + str(elapsed) + ")")
            except Empty as e:
                pass
            except Exception as e:
                logging.warning("[runner" + str(runner_id) + "] error occurred " + str(e))

    def new_invoker(self, func):
        invoker = InvokerImpl.create(func)
        invoker = AsyncInvokerWrapper.create(invoker, self)
        return invoker


