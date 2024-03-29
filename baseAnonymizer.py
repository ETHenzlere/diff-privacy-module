"""Module that handles the full Anonymization pipeline
"""

import sys
import re
import json
import time
import jpype
import jpype.imports
import jaydebeapi
import pandas as pd
from snsynth import Synthesizer
import numpy as np
import visu as vs
import preprocessing as pre
import sensitive as post
import xmlWriter as xml


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
            if dType == "TIMESTAMP" or dType == "DATE" or dType == "TIME":
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
        df[name] = pd.to_datetime(df[name], format="mixed")

    tuples = [tuple(x) for x in df.values]

    if len(timestampIndexes):
        import java

        for i, tup in enumerate(tuples):
            li = list(tup)
            for j in timestampIndexes:
                if pd.isnull(li[j]):
                    li[j] = None
                else:
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


def anonymize(dataset: str, anonConfig: dict, sensConfig: dict, contConfig: dict):
    dropCols = anonConfig["hide"]
    alg = anonConfig["alg"]
    eps = float(anonConfig["eps"])
    preEps = float(anonConfig["preEps"])
    cat = anonConfig["cat"]
    cont = anonConfig["cont"]
    ordi = anonConfig["ord"]
    savedColumns = []
    savedColumnsIndexes = []

    if dropCols:
        savedColumns, savedColumnsIndexes = getDroppableInfo(dropCols, dataset)
        dataset = dataset.drop(dropCols, axis=1)

    nullableFlag = dataset.isnull().values.any()

    
    synthFrame = pd.DataFrame()

    if(eps > 0):
        synth = Synthesizer.create(alg, epsilon=eps, verbose=True)
        startTime = time.perf_counter()
        if contConfig:
            sample = synth.fit_sample(
                dataset,
                preprocessor_eps=preEps,
                categorical_columns=cat,
                continuous_columns=cont,
                ordinal_columns=ordi,
                transformer=pre.getTransformer(dataset, alg, cat, cont, ordi, contConfig),
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

        endTime = time.perf_counter()
        print(f"Process took: {(endTime-startTime):0.2f} seconds")

    else:
        print("Epsilon = 0. Anonymization will return the original data")
        synthFrame = dataset

    try:
        vs.generateVisu(dataset, synthFrame.copy(deep=True))
    except:
        print("An exception occurred while trying to visualize")

    # Stitching the Frame back to its original form
    if dropCols:
        for ind, col in enumerate(dropCols):
            synthFrame.insert(savedColumnsIndexes[ind], col, savedColumns[col])
    if sensConfig:
        post.fakeDataset(synthFrame,sensConfig)

    try:
        if anonConfig.get("table"):
            xml.parse(anonConfig, sensConfig, contConfig)
    except:
        print("An exception occurred while trying to parse the XML config")

    return synthFrame


def anonymizeDB(
    jdbcConfig: dict, anonConfig: dict, sensConfig: dict | None, contConfig: dict | None
):
    driver = jdbcConfig["driver"]
    url = jdbcConfig["url"]
    username = jdbcConfig["username"]
    password = jdbcConfig["password"]
    jar = jdbcConfig["jarPath"]

    jpype.startJVM(classpath=[jar])

    conn = jaydebeapi.connect(driver, url, [username, password])

    curs = conn.cursor()
    table = anonConfig["table"]
    dataset, timestampIndexes = dfFromTable(curs, table)

    datasetAnon = anonymize(dataset, anonConfig, sensConfig, contConfig)

    # Create empty table called ANON
    anonTableName = table + "_anonymized"

    curs.execute(f"DROP TABLE IF EXISTS {anonTableName}")

    createTableQuery = f"CREATE TABLE {anonTableName} AS TABLE {table} WITH NO DATA"
    curs.execute(createTableQuery)
    populateAnonFromDF(curs, datasetAnon, anonTableName, timestampIndexes)
    curs.close()
    conn.close()


def main():
    """Entry method"""
    if len(sys.argv) < 5:
        print(
            "Not enough arguments provided: <jdbcConfig> <anonConfig> <sensConfig> <contConfig>"
        )
        return

    jdbcConfig = json.loads(sys.argv[1])
    anonConfig = json.loads(sys.argv[2])
    sensConfig = json.loads(sys.argv[3])
    contConfig = json.loads(sys.argv[4])

    anonymizeDB(jdbcConfig, anonConfig, sensConfig, contConfig)
    return


if __name__ == "__main__":
    main()
