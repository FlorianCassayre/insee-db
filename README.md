# insee-db

## Introduction

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

Le tableau suivant résume l'espace disque utilisé par chaque fichier pour les `24 811 645` lignes importés :

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
Il est possible d'augmenter la taille de la pile de la JVM au-delà de ce que la mémoire vive peu offrir, mais avec pour conséquence un temps de calcul accru.
