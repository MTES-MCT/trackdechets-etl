from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from datetime import datetime


@task()
def initDir() -> str:
    import os

    tmpDataDir: str = Variable.get("TMP_DATA_DIR_BASE") + str(datetime.now()) + '/'
    Variable.set('TMP_DATA_DIR', tmpDataDir)

    os.mkdir(tmpDataDir)
    return tmpDataDir


@task()
def getIrepData(tmpDataDir) -> str:
    import pandas as pd

    irepPath = Variable.get("IREP_PATH")
    df = pd.read_csv(
        irepPath,
        usecols=['identifiant', 'numero_siret'],
        sep=';',
        dtype={
            'numero_siret': str,
            'identifiant': str
        })
    df.rename(columns={'identifiant': 's3ic', 'numero_siret': 'siret'})
    df.drop_duplicates(inplace=True)
    print("Longueur dataframe IREP : " + str(len(df)))
    df.to_csv(tmpDataDir + "irep.csv")

    return tmpDataDir + "irep.csv"


@task()
def getGerepData(tmp_data_dir) -> str:
    import pandas as pd

    gerepPath = Variable.get("GEREP_PATH")
    df = pd.read_csv(
        gerepPath,
        usecols=['Code établissement', 'Numero Siret'],
        dtype={
            'Code établissement': str,
            'Numero Siret': str
        })
    df.rename(columns={'Code établissement': 's3icId', 'Numero Siret': 'siret'}, inplace=True)
    df.drop_duplicates(inplace=True)

    print("Longueur dataframe GEREP : " + str(len(df)))
    df.to_csv(tmp_data_dir + "gerep.csv")

    return tmp_data_dir + "gerep.csv"


@task()
def downloadIcpeData(tmp_data_dir) -> str:
    import requests

    icpeTarPath = tmp_data_dir + 'icpe.tar.gz'

    icpeUrl = Variable.get("ICPE_URL")
    icpeData = requests.get(icpeUrl, allow_redirects=True)
    open(icpeTarPath, 'wb').write(icpeData.content)

    return icpeTarPath


@task()
def extractIcpeFile(icpeTarPath) -> str:
    import os
    import tarfile

    tmpDataDir = Variable.get('TMP_DATA_DIR')

    # https://stackoverflow.com/a/37474942
    tar = tarfile.open(icpeTarPath, 'r:gz')
    oriFileName = 'IC_etablissement.csv'
    oriFilePath = tmpDataDir + oriFileName
    icpePath = tmpDataDir + 'icpe_ori.csv'
    for member in tar.getmembers():
        if member.name == oriFileName:
            tar.extract(member, path=tmpDataDir)
            os.rename(oriFilePath, icpePath)
    if os.path.exists(icpePath) and os.path.getsize(icpePath) > 0:
        print('ICPE data file extracted successfully.')
    else:
        raise AirflowFailException
    return icpePath


@task()
def addIcpeHeaders(icpePath) -> str:
    import pandas as pd

    tmpDataDir = Variable.get('TMP_DATA_DIR')
    icpe_with_headers = '{}icpe_{}.csv'.format(tmpDataDir, str(datetime.time(datetime.now())))

    pd.read_csv(icpePath, sep=';', header=1, dtype={'siret': str, 'codePostal': str, 'codeCommune': str}, names=[
        's3icId',
        'siret',
        'x',
        'y',
        'region',
        'raisonSociale',
        'codeCommune',
        'codePostal',
        # 1 = en construction, 2 = en fonctionnement, 3 = à l'arrêt, 4 = cessation déclarée, 5 = Récolement fait
        'etatActivite',
        'codeApe',
        'nomCommune',
        'seveso',
        'regime',
        'prioriteNationale',
        # cf. biblio https://aida.ineris.fr/node/193
        'ippc',
        # Etablissement soumis à la déclaration annuelle d'émissions polluantes et de déchets
        'declarationAnnuelle',
        # IN = industrie, BO = bovins, PO = porcs, VO = volailles, CA = carrières
        'familleIc',
        # 1 + 1 = DREAL, etc.
        'baseIdService',
        'natureIdService',
        'adresse1',
        'adresse2',
        'dateInspection',
        # Sites et sols pollués:
        'indicationSsp',
        'rayon',
        'precisionPositionnement'
    ]).to_csv(icpe_with_headers, sep=',')

    return icpe_with_headers


@task()
def siretisationIcpe(icpePath, irepPath, gerepPath) -> str:
    import pandas as pd

    tmpDataDir = Variable.get('TMP_DATA_DIR')
    icpe = pd.read_csv(
        icpePath,
        keep_default_na=False,
        index_col='s3icId'
    )

    irep = pd.read_csv(irepPath, names=['id', 's3icId', 'siret'], index_col='s3icId')
    gerep = pd.read_csv(gerepPath, index_col='s3icId')
    # icpe = icpe.join(irep, how='left')

    icpe_siretisation = '{}icpe_{}.csv'.format(tmpDataDir, str(datetime.time(datetime.now())))
    icpe.to_csv(icpe_siretisation)
    return icpe_siretisation


@task()
def siretisationStats(siretisation_path):
    import pandas as pd
    icpe = pd.read_csv(siretisation_path, dtype={'siret': str}, keep_default_na=False)
    end_with_zero = len(icpe.loc[icpe['siret'].str.endswith('00000')].index)
    empty_siret = len(icpe.loc[icpe['siret'] == ''].index)
    total = len(icpe.index)

    stats = f'''
        sirets terminés par 00000 = {end_with_zero}
        sirets vides = {empty_siret}
        total = {total}
    '''
    return stats


@dag(start_date=datetime(2021, 1, 1),
     schedule_interval=None,
     user_defined_macros={},
     catchup=False)
def icpeSiretisation():
    init_dir = initDir()
    get_gerep_data = getGerepData(init_dir)
    get_irep_data = getIrepData(init_dir)
    download_icpe_data = downloadIcpeData(init_dir)
    get_icpe_data = extractIcpeFile(download_icpe_data)
    add_icpe_headers = addIcpeHeaders(get_icpe_data)
    icpe_siretise = siretisationIcpe(add_icpe_headers, get_irep_data, get_gerep_data)
    siretisationStats(icpe_siretise)




icpe_siretisation_etl = icpeSiretisation()
