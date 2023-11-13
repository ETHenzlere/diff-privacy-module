"""A Helper module handling the faking of sensitive columns
"""
from faker import Faker


def fakeColumn(dataset, col, locales, method, seed=0):
    """Method that generates fake values for columns that are considered sensitive

    Args:
        dataset (DataFrame): A Pandas dataframe holding anonymized data
        col (str): The name of the column
        locales (str[]): A list of locales
        method (str): A string matching the desired faker method
        seed (int, optional): Seed for the fake values. Defaults to 0.

    Returns:
        dict: Mapping from original to fake values
    """
    fake = Faker(locales)

    fake.seed_instance(seed)

    sensDict = {}

    dataset[col] = dataset[col].astype(str)

    try:
        fakerFunc = getattr(fake.unique, method)
        exists = True
    except AttributeError:
        exists = False
        print("Faker method '" + method + "' not found. Resorting to random String")
        fakerFunc = fake.unique.pystr

    for val in dataset[col].unique():
        if exists:
            sensDict[val] = fakerFunc()
        else:
            maxLen = len(val)
            sensDict[val] = fakerFunc(min_chars=1, max_chars=maxLen)

    dataset[col] = dataset[col].map(sensDict)

    fake.unique.clear()

    return dataset, sensDict
