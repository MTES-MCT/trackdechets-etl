from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from os.path import join


@task()
def init_dir() -> str:
    import os

    tmp_data_dir: str = join(Variable.get("TMP_DATA_DIR_BASE"), 'tmp' + str(datetime.now()), '')
    Variable.set('TMP_DATA_DIR', tmp_data_dir)

    os.mkdir(tmp_data_dir)
    return tmp_data_dir


@task()
def download_icpe_data(tmp_data_dir) -> str:
    import requests

    icpe_tar_path = join(tmp_data_dir, 'icpe.tar.gz')

    icpe_url: str = Variable.get("ICPE_URL")

    # If the file is on the local filesystem (testing env), copy instead of downloading
    if icpe_url.startswith('/'):
        from shutil import copyfile
        copyfile(icpe_url, icpe_tar_path)
    else:
        icpe_data = requests.get(icpe_url, allow_redirects=True)
        open(icpe_tar_path, 'wb').write(icpe_data.content)

    return icpe_tar_path


@task()
def extract_icpe_files(icpe_tar_path) -> list:
    import tarfile

    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    # https://stackoverflow.com/a/37474942
    tar = tarfile.open(icpe_tar_path, 'r:gz')
    files_to_extract = [
        'IC_etablissement.csv',
        'IC_installation_classee.csv',
        'IC_ref_nomenclature_ic.csv'
    ]
    for member in tar.getmembers():
        if member.name in files_to_extract:
            tar.extract(member, path=tmp_data_dir)

    return files_to_extract


@task()
def add_icpe_headers(icpe_files: list) -> dict:
    import pandas as pd

    tmp_data_dir = Variable.get('TMP_DATA_DIR')
    now = str(datetime.time(datetime.now()))

    def makeNewFilename(ori) -> str:
        return join(tmp_data_dir, '{}_{}.pkl'.format(ori, now))

    options = {
        "IC_etablissement.csv": {
            'names': [
                'codeS3ic',
                's3icNumeroSiret',
                'x', 'y', 'region',
                'nomEts',
                'codeCommuneEtablissement', 'codePostal',
                # 1 = en construction, 2 = en fonctionnement, 3 = à l'arrêt, 4 = cessation déclarée, 5 = Récolement fait
                'etatActivite',
                'codeApe', 'nomCommune',
                'seveso', 'regime',
                'prioriteNationale',
                # cf. biblio https://aida.ineris.fr/node/193
                'ippc',
                # Etablissement soumis à la déclaration annuelle d'émissions polluantes et de déchets
                'declarationAnnuelle',
                # IN = industrie, BO = bovins, PO = porcs, VO = volailles, CA = carrières
                'familleIc',
                # 1 + 1 = DREAL, etc.
                'baseIdService',
                'natureIdService', 'adresse1' 'adresse2', 'dateInspection',
                # Sites et sols pollués:
                'indicationSsp',
                'rayon', 'precisionPositionnement'
            ],
            'dtype': {'codeS3ic': str, 's3icNumeroSiret': str, 'codePostal': str, 'codeCommuneEtablissement': str},
            'parse_dates': ['dateInspection'],
            'usecols': ['codeS3ic', 's3icNumeroSiret', 'nomEts', 'familleIc', 'regime', 'seveso'],
            'index_col': False
        },
        'IC_installation_classee.csv': {
            'names': [
                'codeS3ic', 'id', 'volume', 'unite', 'date_debut_exploitation', 'date_fin_validite',
                'statut_ic', 'id_ref_nomencla_ic'
            ],
            'dtype': {
                'codeS3ic': str, 'id': str, 'volume': float, 'statut_ic': str
            },
            'parse_dates': ['date_debut_exploitation', 'date_fin_validite'],
            'index_col': False,
            'usecols': False
        },
        'IC_ref_nomenclature_ic.csv': {
            'names': [
                'id', 'rubrique_ic', 'famille_ic', 'sfamille_ic', 'ssfamille_ic', 'alinea', 'libellecourt_activite',
                'id_regime', 'envigueur', 'ippc'
            ],
            'dtype': {
                'rubrique_ic': str,
                'alinea': str,
                'id_regime': str,
                'envigueur': int,
                'ippc': int
            },
            'parse_dates': [],
            'index_col': False,
            'usecols': False
        }

    }

    icpe_with_headers = {}

    for file in icpe_files:
        new_filename = join(tmp_data_dir, makeNewFilename(file))
        icpe_with_headers[file] = new_filename
        usecols = (options[file]['usecols'] or options[file]['names'])
        print(options[file]['index_col'])
        df = pd.read_csv(join(tmp_data_dir, file), sep=';', header=None, dtype=options[file]['dtype'],
                         parse_dates=options[file]['parse_dates'],
                         names=options[file]['names'],
                         index_col=options[file]['index_col'],
                         dayfirst=True)

        df = df[usecols]
        print(df)
        df.to_pickle(new_filename)

    return icpe_with_headers


@task()
def enrich_rubriques(icpe_files: dict) -> str:
    import pandas as pd

    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    rubriques = pd.read_pickle(icpe_files['IC_ref_nomenclature_ic.csv'])
    rubriques['rubrique_ic_alinea'] = rubriques['rubrique_ic'] + '_' + rubriques['alinea']
    rubriques['rubrique_ic_alinea'] = rubriques['rubrique_ic_alinea'].fillna('')
    # rubriques = rubriques[rubriques['rubrique_ic_alinea'].str.startswith('27')]

    rubriques_pickle_path = join(tmp_data_dir, 'rubriques.pkl')
    rubriques.to_pickle(rubriques_pickle_path)

    return rubriques_pickle_path


@task()
def enrich_installations(icpe_files: dict) -> str:
    import pandas as pd

    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    installations_pickle_path = join(tmp_data_dir, 'installations.pkl')

    etablissements = pd.read_pickle(icpe_files['IC_etablissement.csv'])
    installations = pd.read_pickle(icpe_files['IC_installation_classee.csv'])

    print(installations)
    print(etablissements)

    installations = installations.merge(etablissements, left_on='codeS3ic', right_on='codeS3ic', how='left')

    def setValue(value, reference_dict):
        if isinstance(value, str):
            result = ''
            try:
                result = reference_dict[value]
            except KeyError:
                print('Value ' + value + ' not understood. Expecting: ' + ', '.join(reference_dict.keys()))
            return result

    # Seveso label
    lib_seveso = {
        'S': 'Seveso',
        'NS': 'Non Seveso',
        'SB': 'Seveso Seuil Bas',
        'SH': 'Seveso Seuil Haut',
        'H': 'Seveso Seuil Haut',
        'B': 'Seveso Seuil Bas'
    }

    installations['lib_seveso'] = [setValue(x, lib_seveso) for x in installations['seveso']]

    # famille IC label
    famille_ic = {
        'IN': 'Industries',
        'BO': 'Bovins',
        'PO': 'Porcs',
        'VO': 'Volailles',
        'CA': 'Carrières'
    }
    installations['famille_ic_libelle'] = [setValue(x, famille_ic) for x in installations['familleIc']]

    # Régime label
    regime = {
        'A': 'Soumis à Autorisation',
        'E': 'Enregistrement',
        'D': 'Soumis à Déclaration',
        'DC': 'Soumis à Déclaration avec Contrôle périodique',
        'NC': 'Inconnu'
    }
    installations['libRegime'] = [setValue(x, regime) for x in installations['regime']]

    print("Installations after enrichment:")
    print(installations)
    installations.to_pickle(installations_pickle_path)

    return installations_pickle_path


@task()
def make_stats(installations_pickle_path, rubriques_pickle_path):
    import pandas as pd
    installations = pd.read_pickle(installations_pickle_path)
    rubriques = pd.read_pickle(rubriques_pickle_path)

    empty_siret = len(installations.loc[installations['s3icNumeroSiret'] == ''].index)
    total = len(installations.index)

    rubriques = rubriques[rubriques['rubrique_ic_alinea'].str.startswith('27')]
    installations = installations.merge(rubriques, left_on='id_ref_nomencla_ic', right_on='id', how='inner')

    print(installations.columns)

    # installations with rubriques that are relevant for trackdechets
    # TODO complete the list
    rubriques_trackdechets = ['2718_1', '2718_2', '2712', "2712_1", "2712_1a", "2712_1b", "2712_2", "2712_3", "2712_3a", "2712_3b",
                              "2710", "2710_1", "2710_1a", "2710_1b",
                              "2760_1", "2790", "2790_1", "2790_1a", "2790_1b", "2790_2"]
    # others = ["2710_2", "2710_2a", "2710_2b", "2710_2c",  # Déchetteries non-dangereux
    #         ]

    installations_trackdechets: pd.DataFrame = installations.loc[
        installations['rubrique_ic_alinea'].isin(rubriques_trackdechets)]
    installations_trackdechets.to_csv(join(Variable.get("TMP_DATA_DIR_BASE"), 'installations_td.csv'))
    nb_installations_trackdechets = len(installations_trackdechets.value_counts(subset='codeS3ic', dropna=True).index)

    sirets_trackdechets = len(installations_trackdechets.value_counts(subset=['s3icNumeroSiret'], dropna=True).index)

    stats = f'''
    Installations déchets dangereux
        sans siret = {nb_installations_trackdechets - sirets_trackdechets}
        avec siret = {sirets_trackdechets}
        total = {nb_installations_trackdechets}
    '''
    return stats


@task()
def load_to_database(installations_pickle_path, rubriques_pickle_path) -> dict:
    from sqlalchemy import create_engine
    import pandas as pd

    pg_user = Variable.get('PGSQL_USER')
    pg_password = Variable.get('PGSQL_PASSWORD')
    pg_host = Variable.get('PGSQL_HOST')
    pg_port = Variable.get('PGSQL_PORT')
    pg_database = Variable.get('PGSQL_DATABASE')
    pg_connection_string = Variable.get('PGSQL_CONNECTION_STRING', default_var=False)
    pg_schema = Variable.get('PGSQL_SCHEMA')
    table_installations = Variable.get('TABLE_INSTALLATIONS')
    table_rubriques = Variable.get('TABLE_RUBRIQUES')

    engine_string = pg_connection_string or '{}:{}@{}:{}/{}'.format(pg_user, pg_password, pg_host, pg_port, pg_database)

    engine = create_engine('postgresql+psycopg2://' + engine_string)

    installations = pd.read_pickle(installations_pickle_path)
    print(installations)
    installations.to_sql(table_installations, con=engine, schema=pg_schema, if_exists='replace', chunksize=3)

    rubriques = pd.read_pickle(rubriques_pickle_path)

    print(rubriques)
    rubriques.to_sql(table_rubriques, con=engine, schema=pg_schema, if_exists='replace')

    return (
        {
            'Installation': installations_pickle_path,
            'Rubrique': rubriques_pickle_path
        }
    )


@dag(start_date=datetime(2021, 1, 1),
     schedule_interval=None,
     user_defined_macros={},
     catchup=False)
def icpe_etl_dag():
    tmp_data_dir = init_dir()
    icpe_tar_path = download_icpe_data(tmp_data_dir)
    get_icpe_data = extract_icpe_files(icpe_tar_path)
    icpe_with_headers = add_icpe_headers(get_icpe_data)
    rubriques_pickle_path = enrich_rubriques(icpe_with_headers)
    installations_pickle_path = enrich_installations(icpe_with_headers)
    make_stats(installations_pickle_path, rubriques_pickle_path)
    load_to_database(installations_pickle_path, rubriques_pickle_path)


icpe_etl = icpe_etl_dag()
