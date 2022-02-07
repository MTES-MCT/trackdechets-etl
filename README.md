# trackdechets-etl

Scripts de transformation de données dans le cadre de Trackdéchets, orchestrés avec [Airflow](https://airflow.apache.org). Les premiers scripts serviront à préparer les données nécessaires à la production de fiches d'inspection pour les inspecteurs de l'environnement.

## Configuration

Le projet utilise [pipenv](https://pypi.org/project/pipenv/) pour la gestion des dépendances.

1. Installez les dépendances (`apache-airflow` est installé plus tard) :

```bash
pipenv install
```
2. Faites une copie de `sample.env.sh` vers `env.sh`
3. Ajustez les variables de `env.sh` à votre environnement
4. Sourcez `env.sh` afin de déclarer les variables d'environnemnet (`source env.sh`)
5. Installez et démarrez Airflow :

```bash
./start.sh
```

6. Dans Airflow, activez le  DAG icpeETL
7. À droite, cliquez sur le bouton 'Lecture', puis 'Trigger DAG'

Le DAG est exécuté.


