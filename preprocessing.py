""" Module that handles all things related to preprocessing. 
    Modifies constraints for the handling of continuous columns.
"""

from snsynth.transform import BinTransformer, TableTransformer


def getTransformer(
    dataset,
    algorithm: str,
    categorical: list,
    continuous: list,
    ordinal: list,
    continuousConfig: dict,
):
    """Method that returns a TableTransformer object which can be used by DP mechansisms

    Args:
        dataset (DataFrame): Pandas DataFrame
        algorithm (str): Name of the DP-algorithm
        categorical (str[]): List of categorical column names
        continuous (str[]): List of continuous column names
        ordinal (str[]): List of ordinal column names
        continuousConfig (dict): Configuration of continuous columns

    Returns:
        TableTransformer: A transformer object
    """

    transformerStyle = "cube"
    if "gan" in algorithm:
        transformerStyle = "gan"

    tt = TableTransformer.create(
        dataset,
        nullable=dataset.isnull().values.any(),
        categorical_columns=categorical,
        continuous_columns=continuous,
        ordinal_columns=ordinal,
        style=transformerStyle,
        constraints=getConstraints(continuousConfig, dataset),
    )
    print("Instantiated Transformer")
    return tt


def getConstraints(config: dict, dataset):
    """Helper method that forms constraints from a list of continuous columns

    Args:
        config (dict): The continuous column configuration
        dataset (DataFrame): Pandas DataFrame

    Returns:
        dict: A dictionary of constraints that will be applied to each specified column
    """
    constraints = {}

    for colEntry in config:
        colName = colEntry["name"]
        bins = int(colEntry["bins"])

        lower = colEntry["lower"]
        upper = colEntry["upper"]

        minB = None
        maxB = None

        if lower:
            if "." in lower:
                minB = float(lower)
            else:
                minB = int(lower)
        if upper:
            if "." in upper:
                maxB = float(upper)
            else:
                maxB = int(upper)

        nullFlag = dataset[colName].isnull().values.any()
        constraints[colName] = BinTransformer(
            bins=bins, lower=minB, upper=maxB, nullable=nullFlag
        )

    return constraints
