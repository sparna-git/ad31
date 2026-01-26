# Script de conversion des notices AD31


## Structure de projet

1. Dossier `data` : Dossier des resources contenant les fichiers Excel d'origine
   1. Dossier `catalogues`: Dichiers Excel Lieux des Faits, Personnes Morales, Juridictions et ark.
   2. Dossier `templateJSON`: Contient un fichier qui donne la structure JSON à utiliser dans la conversion de données.
   3. Dossier `vocabulaires`: Dossier contenant les fichiers JSON des vocabulaires.
2. Dossier `src`: Dossier contenant les scripts python
   

## Prérequis logiciels

- Python version 3.12
- Poetry version 0.1.0

## Avant de lancer la conversion

- Mettre à jour les fichiers JSON de vocabulaires dans le dossier `vocabulaires` (ce process n'est pas automatique)
- Mettre à jour le fichier de notices dans `data`. Conserver le même nom de fichier.
- Mettre à jour les fichiers annexes dans `data/catalogues`. Conserver les mêmes noms de fichier


## Lancement de la conversion

_Expliquer les commandes pour lancer la conversion..._

- `init.py` : Debut de lancement de  la lecture de notices

[catalog]
    **lieux des faits** : Générer le catalog de tous les lieux
    **Personnes Morales** : Générer le catalog de tous les personnes morales
    **Juridictions** : Générer le catalog des Juridictions