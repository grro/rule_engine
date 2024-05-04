import logging
import inspect
import sys
from typing import List, Dict, Any


def parse_function_annotations(modulename: str) -> Dict[Any, List[str]]:
    function_annotations = {}
    try:
        for func in inspect.getmembers(sys.modules[modulename], inspect.isfunction):
            function_annotations[func[1]] = parse_annotations(func)
    except Exception as e:
        print(e)
    return function_annotations


def parse_annotations(func) -> List[str]:
    annotations = []
    source = inspect.getsource(func[1])
    index = source.find("def ")
    for line in source[:index].strip().splitlines():
        line = line.strip()
        if line.startswith("@when("):
            startIdx = len("@when(")
            endIdx = line.index(')', startIdx)
            ano = line[startIdx: endIdx].strip().strip('"')
            annotations.append(ano)
            logging.debug("annotation '" + ano + "' found")
    return annotations