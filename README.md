# insee-db

## Introduction

(_étapes pour l'installation plus bas_)

L'[INSEE](https://www.insee.fr/) a publié en décembre 2019 les [fichiers des personnes décédées](https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/).
Ces jeux de données recensent de manière _très_ exhaustive les personnes françaises étant décédées depuis **1970**.
Chaque enregistrement contient notamment les données suivantes (du défunt) :

- Nom(s) de famille
- Prénom(s)
- Sexe
- Date et lieu de naissance
- Date et lieu de décès

À ce jour les fichiers totalisent près de **24 millions** d'enregistrements.

Ces données sont très utiles pour les généalogistes car bien qu'elles étaient déjà accessible au grand public sur demande, il n'existait pas encore de fichier global en libre accès.

## Problème

Les fichiers CSV combinés pèsent **2 Go**, dès lors il est presque impossible pour un _utilisateur lambda_ de les utiliser.

Les plateformes de généalogie se sont donc empressé d'entrer les données sur leur système, malheureusement celles-ci n'ont été rendues accessible qu'aux utilisateurs "premium"... Dommage !

Une solution évidente consiste à charger le fichier dans une base de données, mais sans index la recherche reste très longue !
La question est donc de savoir quelles requêtes nous souhaitons réaliser sur les données, puis de construire un index en conséquence.

Dans ce cas il serait intéressant de pouvoir effectuer des recherches avec une combinaison de ces paramètres :

- Nom(s)
- Prénoms
- Lieu (naissance, décès)
- Dates (naissance, décès)

En particulier les champs _noms_ et _prénoms_ sont un peu particuliers car il ne suffit pas seulement de chercher les entrées pour lesquelles la recherche est un préfixe, mais plutôt de chercher les entrées qui sont un sur-ensemble de la requête. Cela signifie que l'on doit aussi considérer tous les ordres possibles.

De même pour les lieux il serait utile de pouvoir rechercher - en plus d'une commune - un département, une région ou un pays.

Enfin il serait pratique de connaître le nombre de résultats obtenus pour une recherche.

## Solution

Afin de répondre à mes besoins personnels, je souhaitais un service qui consomme un minimum de temps-processeur, de mémoire vive et qui puisse tenir sur moins de 20 Go d'espace disque ; ceci afin de pouvoir l'héberger sur un petit [serveur virtuel](https://fr.wikipedia.org/wiki/Serveur_d%C3%A9di%C3%A9_virtuel).

Après des essais infructueux sur des bases SQL j'ai été contraint d'écrire mon propre système de base de données.
Heureusement la tâche a été largement simplifiée par le fait que je n'ai pas eu à supporter que deux types d'opérations :

- Génération de la base de données à partir de fichiers sources CSV
- Requêtes multi-paramétriques sur les données

Les modifications / ajouts / suppressions ne sont donc pas supportés pour des raisons pratiques.
Il faudra donc regénérer la base tous les ans pour tenir compte des nouvelles entrées (hélas).

Le programme a été écrit en [Scala](https://fr.wikipedia.org/wiki/Scala_(langage)) de manière modulaire.
Il utilise extensivement les lecteurs de fichiers à accès aléatoire.
N'importe quel type de requête ne prendra que quelques millisecondes à être complété ce qui est très rapide.

Le tableau suivant résume l'espace disque utilisé par chaque fichier pour les `24 811 645` lignes importées :

| Fichier | Taille |
|:---:|:---|
| `search_index.db` | `17 201 830 719` (17,2 Go) |
| `persons_data.db` | `1 348 397 973` (1,3 Go) |
| `places_index.db` | `23 984 191` (24 Mo) |
| `surnames_index.db` | `16 612 327` (16,6 Mo) |
| `names_index.db` | `4 253 488` (4,2 Mo) |
| `places_data.db` | `887 083` (0,9 Mo) |
| **Total** | **`18 595 965 781`** (**18,6 Go**) |

## Limitations

En pratique cette solution ne répond que partiellement à mon besoin.

- Premièrement, la recherche par dates n'a pas été implémentée car celle-ci produirait un index 50% supérieur à la taille imposée dans le cahier des charges. 

- Deuxièmement, le champ _nom_ a été rendu obligatoire ce qui a permis une réduction de 25% de la taille de l'index.
Cette restriction n'est pas trop gênante dans la mesure où l'intérêt des recherches sans nom de famille est assez limité (même si nécessaires dans certains cas).

- Enfin, l'écriture de la base de données prend du temps et nécessite d'avoir beaucoup de mémoire vive à disposition.
Il est possible d'augmenter la taille de la pile de la JVM au-delà de ce que la mémoire vive peut offrir, mais avec pour conséquence un temps de calcul accru.

## Installation et utilisation

L'installation se décompose en deux étapes ; la première consiste à générer les fichiers de la base de données tandis, la deuxième est la mise en place d'un serveur web capable d'exploiter ces fichiers et de proposer une API REST.

La génération est un processus ponctuel, c'est-à-dire qu'une fois les données produites, la tâche est accomplie.
Dès lors il est possible d'allouer les deux tâches à deux machines distinctes.

Les caractéristiques minimales suivantes sont nécessaires pour chaque tâche :

- Génération :
  - **50Go** d'espace disque (le type de disque a peu d'importance) : fichiers de sortie et espace de stockage temporaire
  - **32Go** de mémoire vive (ou équivalent en swap) : mémoire de travail
- Serveur web :
  - **25Go** d'espace disque (SSD recommandé) : base de données
  - **500Mo** de mémoire vive : JVM
 
Dans les deux cas il vous faudra un environment Scala. Il vous faudra donc les paquets suivants :

- JDK 8+
- [`sbt`](https://www.scala-sbt.org/)
 
(pour le serveur web il est également possible de construire un exécutable `.jar`, dans ce cas seul un JRE est nécessaire)

### Génération des données

Vous devrez **créér** deux dossiers temporaires (initialement vides) qui se rempliront à l'exécution :
- Un dossier pour les fichiers source de l'INSEE (`<dossier_source>`)
- Un dossier de sortie pour les fichiers base de données (`<dossier_sortie>`)

Le chemin peut être relatif, dans ce cas la base correspond au dossier racine du projet.

Si vous souhaitez mettre à jour la base de fichiers sources (en particulier, les nouveaux décès), modifiez le fichier `src/main/scala/MainFetchSources.scala`.

Ensuite, vous pouvez procéder à la génération des données.

1. Commencez par télécharger les fichiers source de l'INSEE :

   `bash ./script/fetch_files.sh <dossier_source>`
2. Une fois les fichiers téléchargés, entrez dans le shell `sbt` :

   `$ sbt -J-Xmx32g`
   
   Au besoin, modifiez la mémoire maximale allouée (ici 32Go).
3. Compilez le code avec `compile` :

   `sbt> compile`
4. Exécutez la procédure de génération :

   `sbt> runMain MainWrite <dossier_sortie> <dossier_source>`
   
   Celle-ci devrait prendre une bonne heure.
   
   Les logs de la sortie standard vous indiqueront le progrès de la tâche.
   Vous pouvez aussi utiliser `watch ls -lah` dans `<dossier_sortie>` pour surveiller la progression.
5. (_optionnel_) Testez la base de données nouvellement générée :

   `sbt> runMain MainTest <dossier_sortie> -s chirac -n jacques`
   
   Si tout s'est passé correctement vous devriez pouvoir lire les données.
   
   Lors d'une mise à jour de la base avec de nouvelles données, veillez à ce que les requêtes dans la nouvelle base produisent plus de résultats que dans l'ancienne (à moins d'un changement dans le code ceci doit toujours être le cas).

### Serveur web

Une fois les données générées dans `<dossier_sortie>` vous pouvez les transférer sur votre serveur web dans un dossier que l'on appelera `<dossier_donnees>`.

1. Entrez à nouveau dans le shell :

  `$ sbt`
2. Démarrez le serveur web :

  `sbt> runMain MainWebserver <dossier_donnees>`
  
Remarque : le serveur web dispose d'un mode maintenance, activable en exécutant :

`sbt> runMain MainWebserverOffline [message]`
