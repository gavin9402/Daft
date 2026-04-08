from __future__ import annotations

import json

import daft


@daft.func
def dictionary_lookup(name: str) -> str:
    dict_path = "mock_dictionary.json"
    with open(dict_path) as f:
        dictionary = json.load(f)

    if name in dictionary:
        return f"Hello {dictionary[name]}"
    else:
        return "Nobody"
