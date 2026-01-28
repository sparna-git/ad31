# Script de conversion des notices AD31

## Structure de projet

1. Dossier `data` : Dossier des resources contenant les fichiers Excel d'origine
   1. Dossier `catalogues`: Fichiers Excel Lieux des Faits, Personnes Morales, Juridictions et ark.
   2. Dossier `templateJSON`: Contient un fichier qui donne la structure JSON à utiliser dans la conversion de données.
   3. Dossier `vocabulaires`: Dossier contenant les fichiers JSON des vocabulaires.
2. Dossier `src`: Dossier contenant les scripts python
3. Fichier de configuration `config.yml` : Contient la liste de tous les fichiers sources à utiliser pour la conversion de données et le dossier où seront stockés les fichiers convertis. Voir le fichier config.yml 
4. Fichier `AD31Convert.py` : Fichier debut de la conversion de données.


## Prérequis logiciels

- Python version 3.12 +
- Poetry version 0.1.0

## Installation des logiciels

   * Python [https://www.python.org/downloads/] 
  
     * Windows
       * Télécharger le fichier: [https://www.python.org/ftp/python/pymanager/python-manager-25.2.msix]
       * Lancer le fichier python-manager-25.2.msix et suivi les pas demandant 
     * Linux
         Lance l'instruction suivant: `sudo apt install python3`

   * Poetry: 
  
     * Installation avec Linux, Windows (WSL) 
      Lancer l'instruction: 
         `curl -sSL https://install.python-poetry.org | python3 -`

## mise à jours poetry

   C'est important d'utiliser le fichier `pyproject.toml` qui est dans le dossier principal avant de mettre à jour poetry, parce que dans le fichier existe tous les outils qui seront utilisés pour développers les tâchés de conversion.

   1. Ouvirir la terminal ligne des commandes
   2. aller au dossier ./ad31/app
   3. mettre à jour les script python avec la commande: `poetry update`

## Avant de lancer la conversion


- Mettre à jour les fichiers JSON de vocabulaires dans le dossier `vocabulaires` (ce process n'est pas automatique) [Voir: Generer un fichier json à partir d'un fichier de vocabulaires Excel.]
- Mettre à jour le fichier de notices dans `data`. Conserver le même nom de fichier.
- Mettre à jour les fichiers annexes dans `data/catalogues`. Conserver les mêmes noms de fichier

## Lancement de la conversion

   Le lancement du script de conversion, on va utiliser la commande suivant:
      `poetry run python [nom du fichier python principal]`
   Exemple de lancement:
      
      ```py
         poetry run python AD31Convert.py
      ```

   Résultat:
      Les fichiers de conversion seront stockés dans le dossier parametre dans le fichier config.yaml.

      ```yml
         output:
            lieux: "output/lieux"
            notices: "output/notices"
      ```
      > La conversion genere 2 dossier
         - Un dossier `lieux` du résultat de la conversion de Lieux (un fichier en format turtle)
         - Un dossier `notices` le contenu est une liste de notices en format turtle.


[catalog]
    **lieux des faits** : Générer le catalog de tous les lieux
    **Personnes Morales** : Générer le catalog de tous les personnes morales
    **Juridictions** : Générer le catalog des Juridictions


# Generer un fichier json à partir d'un fichier de vocabulaires Excel.

Le processus est developper de façon manunuelle, on va utiliser les colonnes `Concept URI`, `skos:notation`, `skos:prefLabel` de chaque fichier de vocabulaire.

Comment exemple, on va utiliser le fichier de sexe de personnes.

1. La colonne `skos:prefLabel` sera notre clé principal.
2. Concept URI et `skos:notation` valeurs qui seront include dans la structure JSON.

Exemple:

Fichier Excel

| concept URI | skos:notation | skos:prefLabel |
|-------------|---------------|----------------|
| type:sxhom  | sxhom         | Homme          |
| type:sxfem  | sxfem         | Femme          |
| type:sxind  | sxind         | Indéterminé    |

Fichier Json
```json
{
   "Homme":  {
        "Concept URI": "type:sxhom",
        "skos:notation": "sxhom"
    },
    "Femme": {
        "Concept URI": "type:sxfem",
        "skos:notation": "sxfem"
    },
    "Indéterminé": {
        "Concept URI": "type:sxind",
        "skos:notation": "sxind"
    }
}
```
