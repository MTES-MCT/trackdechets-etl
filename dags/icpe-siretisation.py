from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from os.path import join


@task()
def initDir() -> str:
    import os

    tmpDataDir: str = join(Variable.get("TMP_DATA_DIR_BASE"), 'tmp' + str(datetime.now()), '')
    Variable.set('TMP_DATA_DIR', tmpDataDir)

    os.mkdir(tmpDataDir)
    return tmpDataDir


@task()
def downloadIcpeData(tmp_data_dir) -> str:
    import requests

    icpeTarPath = join(tmp_data_dir, 'icpe.tar.gz')

    icpeUrl: str = Variable.get("ICPE_URL")

    # If the file is on the local filesystem (testing env), copy instead of downloading
    if icpeUrl.startswith('/'):
        from shutil import copyfile
        copyfile(icpeUrl, icpeTarPath)
    else:
        icpeData = requests.get(icpeUrl, allow_redirects=True)
        open(icpeTarPath, 'wb').write(icpeData.content)

    return icpeTarPath


@task()
def extractIcpeFiles(icpeTarPath) -> list:
    import tarfile

    tmpDataDir = Variable.get('TMP_DATA_DIR')

    # https://stackoverflow.com/a/37474942
    tar = tarfile.open(icpeTarPath, 'r:gz')
    filesToExtract = [
        'IC_etablissement.csv',
        'IC_installation_classee.csv',
        'IC_ref_nomenclature_ic.csv'
    ]
    for member in tar.getmembers():
        if member.name in filesToExtract:
            tar.extract(member, path=tmpDataDir)

    return filesToExtract


@task()
def addIcpeHeaders(icpeFiles: list) -> dict:
    import pandas as pd

    tmpDataDir = Variable.get('TMP_DATA_DIR')
    now = str(datetime.time(datetime.now()))

    def makeNewFilename(ori) -> str:
        return join(tmpDataDir, '{}_{}.pkl'.format(ori, now))

    options = {
        "IC_etablissement.csv": {
            'names': [
                's3icId',
                'siret',
                'x', 'y', 'region', 'raisonSociale', 'codeCommuneEtablissement', 'codePostal',
                # 1 = en construction, 2 = en fonctionnement, 3 = à l'arrêt, 4 = cessation déclarée, 5 = Récolement fait
                'etatActivite',
                'codeApe', 'nomCommune', 'seveso', 'regime', 'prioriteNationale',
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
            'dtype': {0: str, 'siret': str, 'codePostal': str, 'codeCommuneEtablissement': str},
            'parse_dates': [],
            'index_col': 0
        },
        'IC_installation_classee.csv': {
            'names': [
                's3icId', 'id_installation_classee', 'volume', 'unite', 'date_debut_exploitation',  'date_fin_validite', 'statut_ic', 'id_ref_nomencla_ic'
            ],
            'dtype': {
                's3icId': str, 'volume': float, 'statut_ic': str
            },
            'parse_dates': ['date_debut_exploitation', 'date_fin_validite'],
            'index_col': 'id_installation_classee'
        },
        'IC_ref_nomenclature_ic.csv': {
            'names': [
                'id', 'rubrique_ic', 'famille_ic', 'sfamille_ic', 'ssfamille_ic', 'alinea', 'libellecourt_activite', 'id_regime', 'envigueur', 'ippc'
            ],
            'dtype': {
                'rubrique_ic': str,
                'alinea': str,
                'id_regime': str,
                'envigueur': int,
                'ippc': int
            },
            'parse_dates': [],
            'index_col': 'id'
        }

    }

    icpeWithHeaders = {}

    for file in icpeFiles:
        newFilename = join(tmpDataDir, makeNewFilename(file))
        icpeWithHeaders[file] = newFilename
        pd.read_csv(join(tmpDataDir, file), sep=';', header=1, dtype=options[file]['dtype'], parse_dates=options[file]['parse_dates'],
                    names=options[file]['names'], index_col=options[file]['index_col'], dayfirst=True) \
            .to_pickle(newFilename)

    return icpeWithHeaders


@task()
def enrichInstallations(icpeFiles: dict, irepFile) -> str:
    import pandas as pd

    tmpDataDir = Variable.get('TMP_DATA_DIR')

    ic_siretise = join(tmpDataDir, 'ic_siretise.pkl')

    etablissements = pd.read_pickle(icpeFiles['IC_etablissement.csv'])[['siret']]
    installations = pd.read_pickle(icpeFiles['IC_installation_classee.csv'])

    installations = installations.merge(etablissements, left_on='s3icId', how='left', right_index=True)

    installations.to_pickle(ic_siretise)

    return ic_siretise


@task()
def siretisationStats(siretisation_path):
    import pandas as pd
    icpe = pd.read_pickle(siretisation_path)
    empty_siret = len(icpe.loc[icpe['siret'] == ''].index)
    total = len(icpe.index)

    stats = f'''
        sirets vides = {empty_siret}
        total = {total}
    '''
    return stats


@task()
def loadToDatabase(ic_siretise, icpeFiles) -> None:
    from sqlalchemy import create_engine
    import pandas as pd

    pgUser = Variable.get('PGSQL_USER')
    pgPassword = Variable.get('PGSQL_PASSWORD')
    pgHost = Variable.get('PGSQL_HOST')
    pgPort = Variable.get('PGSQL_PORT')
    pgDatabase = Variable.get('PGSQL_DATABASE')
    pgSchema = Variable.get('PGSQL_SCHEMA')
    tableInstallations = Variable.get('TABLE_INSTALLATIONS')
    tableRubriques = Variable.get('TABLE_RUBRIQUES')

    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(pgUser,pgPassword,pgHost,pgPort,pgDatabase), echo=True)

    pd.read_pickle(ic_siretise).to_sql(tableInstallations, con=engine, schema=pgSchema, if_exists='replace')
    pd.read_pickle(icpeFiles['IC_ref_nomenclature_ic.csv']).to_sql(tableRubriques, con=engine, schema=pgSchema, if_exists='replace')

    installations = engine.execute('SELECT "id_installation_classee" FROM ic_installations').fetchall()
    print('nb installations: ' + str(len(installations)))

    rubriques = engine.execute('SELECT "id" FROM ic_rubriques').fetchall()
    print('nb rubriques: ' + str(len(rubriques)))


@dag(start_date=datetime(2021, 1, 1),
     schedule_interval=None,
     user_defined_macros={},
     catchup=False)
def icpeETL():
    init_dir = initDir()
    download_icpe_data = downloadIcpeData(init_dir)
    get_icpe_data = extractIcpeFiles(download_icpe_data)
    add_icpe_headers = addIcpeHeaders(get_icpe_data)
    ic_siretise = enrichInstallations(add_icpe_headers)
    siretisationStats(ic_siretise)
    loadToDatabase(ic_siretise, add_icpe_headers)


icpe_etl = icpeETL()