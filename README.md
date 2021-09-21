# trackdechets-etl

Scripts de transformation de données dans le cadre de Trackdéchets, orchestrés avec [Airflow](https://airflow.apache.org). Les premiers scripts serviront à préparer les données nécessaires à la production de fiches d'inspection pour les inspecteurs de l'environnement.

## Configuration

Il est recommandé d'utiliser un [environnement virtuel Python](https://docs.python.org/3/tutorial/venv.html)

1. Installez les dépendances (`apache-airflow` est installé plus tard) :

```bash
pip install -r requirements.txt
```
2. Faites une copie de `sample.env.sh` vers `env.sh`
3. Ajustez les variables de `env.sh` à votre environnement
4. Installez et démarrez Airflow :

```bash
./init_airflow.sh
```

