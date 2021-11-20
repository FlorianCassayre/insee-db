#!/usr/bin/env python

import json

def main():
    entries = []
    with open("wikidata-raw.json", "r") as f:
        o = json.load(f)
        for result in o["results"]["bindings"]:
            entry = {}
            for k in result:
                entry[k] = result[k]["value"]
            entries.append(entry)
    with open("wikidata.json", "w") as f:
        json.dump(entries, f, separators=(',', ':'))


if __name__ == "__main__":
    main()
