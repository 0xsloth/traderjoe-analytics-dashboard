from typing import Any
import json


def load_json(path: str) -> Any:
    with open(path, mode="r") as f:
        data = json.load(f)
    return data


def dump_json(data: Any, path: str) -> None:
    with open(path, mode="w") as f:
        json.dump(data, f)
