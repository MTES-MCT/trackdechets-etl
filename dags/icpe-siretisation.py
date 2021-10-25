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
                'codeS3ic': str,  'id': str, 'volume': float, 'statut_ic': str
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

    icpeWithHeaders = {}

    for file in icpeFiles:
        newFilename = join(tmpDataDir, makeNewFilename(file))
        icpeWithHeaders[file] = newFilename
        usecols = (options[file]['usecols'] or options[file]['names'])
        print(options[file]['index_col'])
        df = pd.read_csv(join(tmpDataDir, file), sep=';', header=None, dtype=options[file]['dtype'],
                    parse_dates=options[file]['parse_dates'],
                    names=options[file]['names'],
                    index_col=options[file]['index_col'],
                    dayfirst=True)

        df = df[usecols]
        print(df)
        df.to_pickle(newFilename)

    return icpeWithHeaders


@task()
def enrichInstallations(icpeFiles: dict) -> str:
    import pandas as pd

    tmpDataDir = Variable.get('TMP_DATA_DIR')

    ic_siretise = join(tmpDataDir, 'ic_siretise.pkl')

    etablissements = pd.read_pickle(icpeFiles['IC_etablissement.csv'])
    installations = pd.read_pickle(icpeFiles['IC_installation_classee.csv'])

    print(installations)
    print(etablissements)

    installations = installations.merge(etablissements, left_on='codeS3ic', right_on='codeS3ic', how='left')

    def setValue(value, referenceDict):
        if isinstance(value, str):
            result = ''
            try:
                result = referenceDict[value]
            except KeyError:
                print('Value ' + value + ' not understood. Expecting: ' + ', '.join(referenceDict.keys()))
            return result



    # Seveso label
    libSeveso = {
        'S': 'Seveso',
        'NS': 'Non Seveso',
        'SB': 'Seveso Seuil Bas',
        'SH': 'Seveso Seuil Haut',
        'H': 'Seveso Seuil Haut',
        'B': 'Seveso Seuil Bas'
    }

    installations['libSeveso'] = [ setValue(x, libSeveso) for x in installations['seveso'] ]

    # famille IC label
    familleIc = {
        'IN': 'Industries',
        'BO': 'Bovins',
        'PO': 'Porcs',
        'VO': 'Volailles',
        'CA': 'Carrières'
    }
    installations['familleIc'] = [ setValue(x, familleIc) for x in installations['familleIc'] ]

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
    installations.to_pickle(ic_siretise)


    return ic_siretise


@task()
def siretisationStats(siretisation_path):
    import pandas as pd
    icpe = pd.read_pickle(siretisation_path)
    empty_siret = len(icpe.loc[icpe['s3icNumeroSiret'] == ''].index)
    total = len(icpe.index)

    stats = f'''
        sirets vides = {empty_siret}
        total = {total}
    '''
    return stats


@task()
def loadToDatabase(ic_siretise, icpeFiles) -> dict:
    from sqlalchemy import create_engine
    import pandas as pd

    pgUser = Variable.get('PGSQL_USER')
    pgPassword = Variable.get('PGSQL_PASSWORD')
    pgHost = Variable.get('PGSQL_HOST')
    pgPort = Variable.get('PGSQL_PORT')
    pgDatabase = Variable.get('PGSQL_DATABASE')
    pgConnectionString = Variable.get('PGSQL_CONNECTION_STRING', default_var=False)
    pgSchema = Variable.get('PGSQL_SCHEMA')
    tableInstallations = Variable.get('TABLE_INSTALLATIONS')
    tableRubriques = Variable.get('TABLE_RUBRIQUES')

    engineString = pgConnectionString or '{}:{}@{}:{}/{}'.format(pgUser, pgPassword, pgHost, pgPort, pgDatabase)

    engine = create_engine('postgresql+psycopg2://' + engineString)

    installations = pd.read_pickle(ic_siretise)
    print(installations)
    installations.to_sql(tableInstallations, con=engine, schema=pgSchema, if_exists='replace', chunksize=3)

    rubriques = pd.read_pickle(icpeFiles['IC_ref_nomenclature_ic.csv'])
    print(rubriques)
    rubriques.to_sql(tableRubriques, con=engine, schema=pgSchema, if_exists='replace')

    return (
        {
            'Installation': ic_siretise,
            'Rubrique': icpeFiles['IC_ref_nomenclature_ic.csv']
        }
    )

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
