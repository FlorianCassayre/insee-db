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
get https://www.insee.fr/fr/statistiques/fichier/2540004/nat2021_csv.zip 9af0eb19bc09e99a5197ea569e915b37 && mv nat2021.csv prenoms.csv

# Opposition (blacklist)
wget --quiet https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20220820-170217/fichier-opposition-deces.csv && mv fichier-opposition-deces.csv opposition.csv

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

# Monthly (January-July 2022)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M07.zip 76d627bd5571aaad1c7686a0b0aa015c && sed 's/,/;/g' Deces_2022_M07.csv > deces-2022-m07.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M06.zip dfba8fe19d745c71a999826067c93ebf && mv Deces_2022_M06.csv deces-2022-m06.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M05.zip 4c6814ea8068624f9683e213ecfe9218 && sed 's/,/;/g' Deces_2022_M05.csv > deces-2022-m05.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M04.zip b7ebba09400e65e798080d1786d2cc46 && mv Deces_2022_M04.csv deces-2022-m04.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M03.zip 92bfd06a2b2737afcfbf14c487cfa34e && mv Deces_2022_M03.csv deces-2022-m03.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M02.zip 9910e3c2ff547a1a9f1a4a14f1e9ca10 && mv Deces_2022_M02.csv deces-2022-m02.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022_M01.zip c2904db6805463bc804af0b8d040b8c3 && mv Deces_2022_M01.csv deces-2022-m01.csv

# Yearly (2020-2021)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021.zip ecf476f3d98f62fb907cfe48e3475065 && mv deces_2020.csv deces-2020.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020.zip 73ef602ebc531dc1f44673f5c8cd3f58 && mv Deces_2021.csv deces-2021.csv

# Decennial (1970-2019)
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2010-2019-csv.zip d8eab639bd14a14e68d8922803f24fc0 && for y in {2010..2019}; do mv "Deces_$y.csv" "deces-$y.csv"; done
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2000-2009-csv.zip 0ac8482703fdba59c04b0b2fb27bfc06
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1990-1999-csv.zip 9716ffafb09a4efd61c6b667a5092ffc
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1980-1989-csv.zip 12e3e802b61022c3fc34de014988e180
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1970-1979-csv.zip c7fb5418a8061d5ffa106d69769cd0b2

cd ..

echo "Done"
