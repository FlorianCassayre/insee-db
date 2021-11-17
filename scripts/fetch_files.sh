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
    wget "$1" --quiet || return $?
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
    zipinfo -1 "$FILE" || return $?
    unzip -qq "$FILE" || return $?
    rm "$FILE" || return $?
}

# Names
get https://www.insee.fr/fr/statistiques/fichier/2540004/nat2020_csv.zip 9af0eb19bc09e99a5197ea569e915b37 && mv nat2020.csv prenoms.csv

# Opposition (blacklist)
wget --quiet https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20211113-141955/fichier-opposition-deces.csv && mv fichier-opposition-deces.csv opposition.csv

mkdir places
cd places

# Places
get https://www.insee.fr/fr/statistiques/fichier/5057840/commune2021-csv.zip 9f365b57b6cdcba50f8982ebb3a75056 && mv commune2021.csv communes.csv
get https://www.insee.fr/fr/statistiques/fichier/5057840/departement2021-csv.zip 11fa146155552d879c90831f9ff3bdb2 && mv departement2021.csv departements.csv
get https://www.insee.fr/fr/statistiques/fichier/5057840/region2021-csv.zip 2fa88545050ce208d1449218a1b5b13a && mv region2021.csv regions.csv
get https://www.insee.fr/fr/statistiques/fichier/5057840/pays2021-csv.zip c8de3b786426cd475b04d64bfce75b0b && mv pays2021.csv pays.csv
get https://www.insee.fr/fr/statistiques/fichier/5057840/mvtcommune2021-csv.zip ab9b5ce9382284d951d8a7e8956b791c && mv mvtcommune2021.csv mouvements.csv

cd ..

mkdir deaths
cd deaths

# Monthly (january-november 2020)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M10.zip a6094fff28170239690bece08bdf1d75 && mv Deces_2021_M10.csv deces-2021-m10.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M09.zip 4e550fa66921de8dc8b5053c10717c61 && mv Deces_2021_M09.csv deces-2021-m09.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M08.zip 6893ea9a39199d3a0588a1ca4089ebad && mv Deces_2021_M08.csv deces-2021-m08.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M07.zip 5ea24632cb52b71e2a6472b4bbeda5ab && mv Deces_2021_M07.csv deces-2021-m07.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M06.zip 3728a9771e1468a46add46a9dbad44a6 && mv Deces_2021_M06.csv deces-2021-m06.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M05.zip 42421de5bc6311cbf046ac11aec9b84e && mv Deces_2021_M05.csv deces-2021-m05.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M04.zip 27196917f89c639a54f6ee65b6596960 && mv Deces_2021_M04.csv deces-2021-m04.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M03.zip 1c120ea549aaefdc611afbcd6fdd8a55 && mv Deces_2021_M03.csv deces-2021-m03.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M02.zip 78a66b317dfa4b052575ce365e853e54 && mv Deces_2021_M02.csv deces-2021-m02.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021_M01.zip 7fdaf3b6e83fda360ef971fd5d7d87b3 && mv Deces_2021_M01.csv deces-2021-m01.csv

# Yearly (2019-2020)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020.zip 73ef602ebc531dc1f44673f5c8cd3f58
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2019.zip 3509ff62cb456d7bc0eab80e50611ea9

# Decennial (1970-2018)
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2010-2018-csv.zip df506b0c298f02f30fdf327f2f89e864
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2000-2009-csv.zip 0ac8482703fdba59c04b0b2fb27bfc06
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1990-1999-csv.zip 9716ffafb09a4efd61c6b667a5092ffc
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1980-1989-csv.zip 12e3e802b61022c3fc34de014988e180
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1970-1979-csv.zip c7fb5418a8061d5ffa106d69769cd0b2

cd ..

echo "Done"
