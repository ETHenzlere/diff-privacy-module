"""Module that handles the full Anonymization pipeline
"""
import sys
import re
from bs4 import BeautifulSoup
import jpype
import jaydebeapi
import pandas as pd
from snsynth import Synthesizer
import numpy as np
import visu as vs


def getTimestampColumns(dbTypeList):
    """A helper function that returns a list of indexes of timestamp-type columns

    Args:
        dbTypeList (any): A list of Database column metadata

    Returns:
        int[]: A list of indexes of timestamp-type columns
    """
    timestampIndexes = []
    for i, entry in enumerate(dbTypeList):
        for dType in entry.values:
            if dType == "TIMESTAMP":
                timestampIndexes.append(i)
    return timestampIndexes


def storageLocation(path):
    """Helper function that returns the folder path from a .csv file

    Args:
        path (string): Path to the .csv file

    Returns:
        string: The storage folder
    """
    temp = re.split(r"^(.+)/([^/]+)$", path)
    baseLoc = ""
    if len(temp) > 1:
        baseLoc = temp[1]

    return baseLoc


def splitString(colNamesString):
    """Helper function that creates a list from a comma separated string

    Args:
        colNamesString (string): A comma separated string or '-1'

    Returns:
        str[]: A list of strings
    """
    return [] if colNamesString == "-1" else list(colNamesString.split(","))


def dfFromTable(curs, table):
    """Helper function that creates a pandas DataFrame from a jaydebe connection

    Args:
        curs (cursor): The connection cursor
        table (str): The name of the table

    Returns:
        Dataframe,string[]: The table as a DataFrame and the indexes of timestamp columns
    """
    curs.execute(f"SELECT * FROM {table}")

    res = curs.fetchall()
    meta = curs.description

    cols = []
    colTypes = []
    for entry in meta:
        cols.append(str(entry[0]))
        colTypes.append(entry[1])
    timestampIndexes = getTimestampColumns(colTypes)

    frame = pd.DataFrame(res, columns=cols)

    return frame, timestampIndexes


def populateAnonFromDF(curs, df, table, timestampIndexes):
    """Helper function to fill a DB table from a DataFrame

    Args:
        curs (cursor): The connection cursor
        df (DataFrame): Pandas DataFrame
        table (string): The name of the table
        timestampIndexes (int[]): A list of indexes of timestamp-type columns
    """
    df = df.replace(np.nan, None)

    for ind in timestampIndexes:
        name = df.columns[ind]
        df[name] = pd.to_datetime(df[name])

    tuples = [tuple(x) for x in df.values]

    if len(timestampIndexes):
        import java

        for i, tup in enumerate(tuples):
            li = list(tup)
            for j in timestampIndexes:
                li[j] = java.sql.Timestamp @ li[j]
            tuples[i] = tuple(li)

    colSlots = f"({','.join('?' for _ in df.columns)})"
    insertSQL = f"insert into {table} values {colSlots}"
    curs.executemany(insertSQL, tuples)


def getDroppableInfo(dropCols, dataset):
    """Helper function that saves droppable columns from anonymization

    Args:
        dropCols (str[]): A list of column names
        dataset (DataFrame): The dataset

    Returns:
        DataFrame,int[]: The saved columns as a DataFrame and a list of the original indexes of the columns
    """
    savedColumns = dataset[dropCols]
    savedColumnsIndexes = []

    for col in dropCols:
        savedColumnsIndexes.append(dataset.columns.get_loc(col))
    return savedColumns, savedColumnsIndexes


def anonymize(
    dataset: str,
    eps: float,
    preEps: float,
    dropCols,
    cat,
    cont,
    ordi,
    alg: str,
):
    """Function that handles the anonymization itself and the result visualization
    Args:
        dataset (DataFrame): Data
        eps (float):Epsilon
        preEps (float): Preprocessor Epsilon
        dropCols (str[]): Columns that will not take part in anonymization
        cat (str[]): Categorical columns
        cont (str[]): Continuous columns
        ordi (str[]): Ordinal columns
        alg (str): Algorithm for Anonymization

    Returns:
        pd.DataFrame: An Anonymized pandas DataFrame
    """
    savedColumns = []
    savedColumnsIndexes = []
    if dropCols:
        savedColumns, savedColumnsIndexes = getDroppableInfo(dropCols, dataset)
        dataset = dataset.drop(dropCols, axis=1)

    nullableFlag = dataset.isnull().values.any()

    synth = Synthesizer.create(alg, epsilon=eps, verbose=True)
    synthFrame = pd.DataFrame()

    if len(cat) == 0 and len(cont) == 0 and len(ordi) == 0:
        sample = synth.fit_sample(
            dataset,
            preprocessor_eps=preEps,
            nullable=nullableFlag,
        )
        synthFrame = pd.DataFrame(sample)
    else:
        sample = synth.fit_sample(
            dataset,
            preprocessor_eps=preEps,
            categorical_columns=cat,
            continuous_columns=cont,
            ordinal_columns=ordi,
            nullable=nullableFlag,
        )
        synthFrame = pd.DataFrame(sample)

    vs.generateVisu(cont, cat, ordi, dataset, synthFrame.copy(deep=True))

    # Stitching the Frame back to its original form
    if dropCols:
        for ind, col in enumerate(dropCols):
            synthFrame.insert(savedColumnsIndexes[ind], col, savedColumns[col])

    return synthFrame


def anonymizeDB(table, eps, preEps, dropCols, cat, cont, ordi, alg):
    """Function that handles the anonymization pipeline for DB Tables.

    Args:
        table (string): Name of the DB Table
        eps (float):Epsilon
        preEps (float): Preprocessor Epsilon
        dropCols (string[]): Columns that will not take part in anonymization
        cat (string[]): Categorical columns
        cont (string[]): Continuous columns
        ordi (string[]): Ordinal columns
        alg (string): Algorithm for Anonymization
    """
    with open("config.xml", "r") as f:
        data = f.read()

    xmlConfig = BeautifulSoup(data, "xml")

    driver = xmlConfig.find("driver").text
    url = xmlConfig.find("url").text
    username = xmlConfig.find("username").text
    password = xmlConfig.find("password").text
    jar = xmlConfig.find("jarPath").text

    jpype.startJVM(classpath=[jar])

    conn = jaydebeapi.connect(driver, url, [username, password])

    curs = conn.cursor()

    dataset, timestampIndexes = dfFromTable(curs, table)

    datasetAnon = anonymize(dataset, eps, preEps, dropCols, cat, cont, ordi, alg)

    # Create empty table called ANON
    anonTableName = table + "_anonymized"

    curs.execute(f"DROP TABLE IF EXISTS {anonTableName}")

    createTableQuery = f"CREATE TABLE {anonTableName} AS TABLE {table} WITH NO DATA"
    curs.execute(createTableQuery)
    populateAnonFromDF(curs, datasetAnon, anonTableName, timestampIndexes)
    curs.close()
    conn.close()


def anonymizeCSV(csvPath, eps, preEps, dropCols, cat, cont, ordi, alg):
    """Function that handles the anonymization pipeline for CSV Files.

    Args:
        csvPath (string): Path to the .csv data
        eps (float):Epsilon
        preEps (float): Preprocessor Epsilon
        dropCols (string[]): Columns that will not take part in anonymization
        cat (string[]): Categorical columns
        cont (string[]): Continuous columns
        ordi (string[]): Ordinal columns
        alg (string): Algorithm for Anonymization
    """
    dataset = pd.read_csv(csvPath, index_col=None)
    originalTypes = dataset.dtypes
    datasetAnon = anonymize(dataset, eps, preEps, dropCols, cat, cont, ordi, alg)
    datasetAnon = datasetAnon.astype(originalTypes)

    baseLoc = storageLocation(csvPath)
    baseLoc += "/DPOut.csv"
    datasetAnon.to_csv(baseLoc, index=False)


def main():
    """Entry method"""
    if len(sys.argv) < 9:
        print(
            "Not enough arguments provided: <tableName> <eps> <preprocessorEps> <dropColumns> <categoricalColumns> <continuousColumns> <ordinalColumns> <algorithm>"
        )
        return

    workloadDecider = sys.argv[1]

    eps = float(sys.argv[2])

    preEps = float(sys.argv[3])

    saveList = sys.argv[4]
    columnsToSave = splitString(saveList)

    catList = sys.argv[5]
    categorical = splitString(catList)

    contList = sys.argv[6]
    continuous = splitString(contList)

    ordList = sys.argv[7]
    ordinal = splitString(ordList)

    algorithm = sys.argv[8]

    if ".csv" in workloadDecider:
        anonymizeCSV(
            workloadDecider,
            eps,
            preEps,
            columnsToSave,
            categorical,
            continuous,
            ordinal,
            algorithm,
        )
    else:
        anonymizeDB(
            workloadDecider,
            eps,
            preEps,
            columnsToSave,
            categorical,
            continuous,
            ordinal,
            algorithm,
        )
    return


if __name__ == "__main__":
    main()
