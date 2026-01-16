# ** Script de conversion des notices AD31 **


> Structure de pojet.

APP: 
    - 

1. Dossier data : Dossier des resources et le fichier des noticies (fichier excel).
   1.1 catalogues: Stoccker les fichiers en excel Lieux des Faits, Personnes Morales, Juridictions et ark.
   1.2 templateJSON: Dosier que a un fichier qui contient la structure JSON à utiliser dans la conversion de données.
   1.3 vocabulaires; Dosier qui a des fichiers json avec des vocabilaires.
2. Dossier src: Dossiers de script python
   

> #Script

"init.py" : Debut de lancement de  la lecture de notices

[catalog]
    **lieux des faits** : Générer le catalog de tous les lieux
    **Personnes Morales** : Générer le catalog de tous les personnes morales
    **Juridictions** : Générer le catalog des Juridictions