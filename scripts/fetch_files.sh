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
get https://www.insee.fr/fr/statistiques/fichier/2540004/nat2021_csv.zip && mv nat2021.csv prenoms.csv

# Opposition (blacklist)
wget --quiet https://www.data.gouv.fr/fr/datasets/r/7bcdfa57-dc50-43a8-beb6-6c76537e7057 && mv 7bcdfa57-dc50-43a8-beb6-6c76537e7057 opposition.csv

mkdir places
cd places

# Places
wget --quiet https://www.insee.fr/fr/statistiques/fichier/6800675/v_commune_2023.csv && mv v_commune_2023.csv communes.csv
wget --quiet https://www.insee.fr/fr/statistiques/fichier/6800675/v_departement_2023.csv && mv v_departement_2023.csv departements.csv
wget --quiet https://www.insee.fr/fr/statistiques/fichier/6800675/v_region_2023.csv && mv v_region_2023.csv regions.csv
wget --quiet https://www.insee.fr/fr/statistiques/fichier/6800675/v_pays_2023.csv && mv v_pays_2023.csv pays.csv
wget --quiet https://www.insee.fr/fr/statistiques/fichier/6800675/v_mvtcommune_2023.csv && mv v_mvtcommune_2023.csv mouvements.csv

cd ..

mkdir deaths
cd deaths

# Monthly (January-July 2023)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M07.zip 7cc46c67850f7007a316bfa67301d995 && mv Deces_2023_M07.csv deces-2023-m07.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M06.zip 579b3781a5f5c78e72ddc2502d187240 && mv Deces_2023_M06.csv deces-2023-m06.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M05.zip d9edc0ae9a4ee4f061133d3bb7e3c40f && mv Deces_2023_M05.csv deces-2023-m05.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M04.zip 680e5e8788a7ee08b219d675e81ec30f && mv Deces_2023_M04.csv deces-2023-m04.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M03.zip 13fde0777c5163a1c857b85806035ee2 && mv Deces_2023_M03.csv deces-2023-m03.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M02.zip d6cc6e23d0b89d7ac090077fd3cb3e20 && mv Deces_2023_M02.csv deces-2023-m02.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2023_M01.zip 55abebf4543bfa1740bba6ec0c056ef5 && mv Deces_2023_M01.csv deces-2023-m01.csv

# Yearly (2020-2022)
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2022.zip f62384440597c24864842b5cf2ffa0af && mv Deces_2022.csv deces-2022.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2021.zip ecf476f3d98f62fb907cfe48e3475065 && mv Deces_2021.csv deces-2021.csv
get https://www.insee.fr/fr/statistiques/fichier/4190491/Deces_2020.zip 73ef602ebc531dc1f44673f5c8cd3f58 && mv deces_2020.csv deces-2020.csv

# Decennial (1970-2019)
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2010-2019-csv.zip d8eab639bd14a14e68d8922803f24fc0 && for y in {2010..2019}; do mv "Deces_$y.csv" "deces-$y.csv"; done
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-2000-2009-csv.zip 0ac8482703fdba59c04b0b2fb27bfc06
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1990-1999-csv.zip 9716ffafb09a4efd61c6b667a5092ffc
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1980-1989-csv.zip 12e3e802b61022c3fc34de014988e180
get https://www.insee.fr/fr/statistiques/fichier/4769950/deces-1970-1979-csv.zip c7fb5418a8061d5ffa106d69769cd0b2

cd ..

echo "Done"
