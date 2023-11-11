"""A Helper module handling the faking of sensitive columns
"""
from faker import Faker


def fakeColumn(dataset, col, locales, method, seed=0):
    """Method that generates fake values for columns that are considered sensitive

    Args:
        dataset (DataFrame): A Pandas dataframe holding anonymized data
        col (string): The name of the column
        locales (string[]): A list of locales
        method (string): A string matching the desired faker method
        seed (int, optional): Seed for the fake values. Defaults to 0.

    Returns:
        dict: Mapping from original to fake values
    """
    fake = Faker(locales)
    fake.seed_instance(seed)

    sensDict = {}

    for val in dataset[col].unique():
        try:
            fakerFunc = getattr(fake.unique, method)
            sensDict[val] = fakerFunc()
        except ():
            print("Faker method '" + method + "' not found. Resorting to random String")
            maxLen = len(val)
            sensDict[val] = fake.unique.pystr(min_chars=1, max_chars=maxLen)
    dataset[col].replace(dict, inplace=True)

    return sensDict
