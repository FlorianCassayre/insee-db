#!/usr/bin/env python

import json
import sys


def main():
    assert len(sys.argv) == 3, "Arguments: <input_file> <output_file>"
    input_file, output_file = sys.argv[1:]

    entries = []
    with open(input_file, "r") as f:
        o = json.load(f)
        for result in o["results"]["bindings"]:
            entry = {}
            for k in result:
                entry[k] = result[k]["value"]
            entries.append(entry)
    with open(output_file, "w") as f:
        json.dump(entries, f, separators=(',', ':'))


if __name__ == "__main__":
    main()
