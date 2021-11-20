#!/bin/bash

set -e

if [ -z "$1" ]
then
  echo "Usage: ./scripts/query_wikidata.sh <output_directory>"
  exit 1
fi

cd "$1"

QUERY=$(cat <<-END

SELECT
?person
?personName
?personNameBirth
?personAltLabel
?personBirthDate
?personDeathDate
?personArticleFr
?personArticleEn
WHERE
{
  ?person wdt:P31 wd:Q5; wdt:P27 wd:Q142.
  OPTIONAL { ?person wdt:P1559 ?personName. }
  OPTIONAL { ?person wdt:P1477 ?personNameBirth. }
  ?person wdt:P569 ?personBirthDate. hint:Prior hint:rangeSafe true.
  ?person wdt:P570 ?personDeathDate. hint:Prior hint:rangeSafe true.
  OPTIONAL { ?personArticleFr schema:about ?person; schema:isPartOf <https://fr.wikipedia.org/> }
  OPTIONAL { ?personArticleEn schema:about ?person; schema:isPartOf <https://en.wikipedia.org/> }

  FILTER(?personDeathDate >= "1970-01-01"^^xsd:dateTime)

  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
}

END
)

curl \
  --header "Accept: application/json" \
  --header "User-Agent: insee-db-fetch-bot/0.1 (https://github.com/FlorianCassayre/insee-db) insee-db/0.1" \
  --data-urlencode "query=$QUERY" \
  "https://query.wikidata.org/sparql" \
  -o "wikidata-raw.json"
