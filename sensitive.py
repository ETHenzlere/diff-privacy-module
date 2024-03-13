"""A Helper module handling the faking of sensitive columns
"""

from faker import Faker


def fakeDataset(dataset, sensConfig: dict):
    for sensCol in sensConfig:
        dataset, _ = fakeColumn(
            dataset,
            sensCol["name"],
            sensCol["method"],
            sensCol["mode"],
            sensCol["locales"],
            sensCol["seed"]
        )
    return dataset


def fakeColumn(dataset, col: str, method: str, mode: str, locales: list, seed=0):

    if len(locales) > 0:
        fake = Faker(locales)
    else:
        fake = Faker()

    fake.seed_instance(seed)

    sensDict = {}

    replacementValues = []
    minValueLength = 0
    maxValueLength = 1

    dataset[col] = dataset[col].astype(str)

    try:
        if mode == "unique":
            fakerFunc = getattr(fake.unique, method)
        else:
            fakerFunc = getattr(fake, method)
        exists = True
    except AttributeError:
        exists = False
        minValueLength = len(min(dataset[col].tolist(), key=len))
        maxValueLength = len(max(dataset[col].tolist(), key=len))
        print("Faker method '" + method + "' not found. Resorting to random String")

    # Fitting mode requires each distinct value to have their own translation
    collection = dataset[col].unique() if mode == "fitting" else dataset[col]

    for val in collection:
        if exists:
            fakeValue = fakerFunc()
            if mode == "fitting":
                sensDict[val] = fakeValue
            else:
                replacementValues.append(fakeValue)
        else:
            if mode == "natural":
                replacementValues.append(
                    fake.pystr(min_chars=minValueLength, max_chars=maxValueLength)
                )
            else:
                fakeString = fake.unique.pystr(
                    min_chars=minValueLength, max_chars=maxValueLength
                ) 
                if mode == "fitting":
                    sensDict[val] = fakeString
                else:
                    replacementValues.append(fakeString)

    if mode == "fitting":
        dataset[col] = dataset[col].map(sensDict)
    else:
        dataset[col] = replacementValues

    fake.unique.clear()

    return dataset, sensDict
