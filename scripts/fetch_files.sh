#!/bin/bash

set -e

if [ -z "$1" ]
then
  echo "Usage: ./scripts/fetch_files.sh <output_directory>"
  exit 1
fi

cd "$1"

if [ "$(ls -A .)" ]; then
  echo "Error: the directory is not empty"
  exit 1
fi

function get() {
    # wget a file and verify integrity
    # $1: the file url
    # $2: the expected hash
    echo "Downloading $1"
    wget "$1" --quiet --show-progress
    FILE=$(ls *.zip)
    SUM=$(md5sum "$FILE" | awk '{ print $1 }')
    if [ "$SUM" != "$2" ]; then
      if [ -z "$2" ]; then
        echo "Info: file has checksum $SUM"
      else
        echo "Error: checksum $SUM doesn't match expectation $2"
        exit 1
      fi
    fi
    echo "Unzipping:"
    zipinfo -1 "$FILE"
    unzip -qq "$FILE"
    rm "$FILE"
}

# Names
get https://www.insee.fr/fr/statistiques/fichier/2540004/nat2019_csv.zip dee96e38282a9f30ea1a862a1eafc9d8 && mv nat2019.csv prenoms.csv

mkdir places
cd places

# Places
get https://www.insee.fr/fr/statistiques/fichier/3720946/communes-01012019-csv.zip d700d426168bd2c8d25bb3557dfc56d4 && mv communes-01012019.csv communes.csv
get https://www.insee.fr/fr/statistiques/fichier/3720946/departement2019-csv.zip 6ddad71eba372341e7c2018aa78d12ef && mv departement2019.csv departements.csv
get https://www.insee.fr/fr/statistiques/fichier/3720946/region2019-csv.zip 4d26e82270ed47cc2982f4cf790f828d && mv region2019.csv regions.csv
get https://www.insee.fr/fr/statistiques/fichier/3720946/pays2019-csv.zip 0a33a4917eff8521f6d629960f985f82 && mv pays2019.csv pays.csv
get https://www.insee.fr/fr/statistiques/fichier/3720946/mvtcommune-01012019-csv.zip 398847495b747496114da817a971959b && mv mvtcommune-01012019.csv mouvements.csv

cd ..

mkdir deaths
cd deaths

# Monthly (january-september 2020)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M09.zip af94c45abe1f8fe0f893aa5d386eb5ab && mv Deces_2020_M09.csv deces-2020-m09.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M08.zip 2c3e2cd9058ca812fca3e161bccd370d && mv Deces_2020_M08.csv deces-2020-m08.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M07.zip 2e527ff6af1f18bdf4bceef510dc49e0 && mv Deces_2020_M07.csv deces-2020-m07.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M06.zip d66cd4c0f6e502031f7dfb762f815a37 && mv Deces_2020_M06.csv deces-2020-m06.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M05.zip 05337c89e4655db51350b282507173d7 && mv Deces_2020_M05.csv deces-2020-m05.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M04.zip 72edc837fbcefb51d2923d95512dca18 && mv Deces_2020_M04.csv deces-2020-m04.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M03.zip b25f800adf0540b4f8d23a9a1b5f5a2d && mv Deces_2020_M03.csv deces-2020-m03.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M02.zip dcea1b9c19e55d58fcb4ad005090d97d && mv Deces_2020_M02.csv deces-2020-m02.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020_M01.zip 55356ab7144c0ab8824d9ae331d7b009

# Yearly (2019)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2019.zip 3509ff62cb456d7bc0eab80e50611ea9

# Decennial (1970-2018)
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2010-2018-csv.zip df506b0c298f02f30fdf327f2f89e864
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2000-2009-csv.zip 0ac8482703fdba59c04b0b2fb27bfc06
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1990-1999-csv.zip 9716ffafb09a4efd61c6b667a5092ffc
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1980-1989-csv.zip 12e3e802b61022c3fc34de014988e180
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1970-1979-csv.zip c7fb5418a8061d5ffa106d69769cd0b2

cd ..

echo "Done"
