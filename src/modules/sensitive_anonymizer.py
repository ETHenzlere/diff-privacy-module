import pandas as pd

from configuration.configurations import SensitiveConfig
from faker import Faker


class SensitiveAnonymizer:
    def __init__(
        self, dataset: pd.DataFrame, sens_config: SensitiveConfig
    ):
        self.dataset = dataset
        self.sens_config = sens_config

    def run_anonymization(self):
        anon_data = self.dataset.copy()
        list_of_mappings = []
        if self.sens_config:
            for col in self.sens_config.columns:
                anon_data, mapping = self.__fake_column(
                    anon_data, col.name, col.method, col.mode, col.locales, col.seed
                )
                list_of_mappings.append(mapping)
                # TODO: Use list of mappings to change templates file
        return anon_data

    def __fake_column(
        self,
        dataset: pd.DataFrame,
        col_name: str,
        method: str,
        mode: str,
        locales: list,
        seed=0,
    ):
        if len(locales) > 0:
            fake = Faker(locales)
        else:
            fake = Faker()

        fake.seed_instance(seed)
        sensDict = {}
        replacementValues = []
        minValueLength = 0
        maxValueLength = 1
        dataset[col_name] = dataset[col_name].astype(str)
        try:
            if mode == "unique":
                fakerFunc = getattr(fake.unique, method)
            else:
                fakerFunc = getattr(fake, method)
            exists = True
        except AttributeError:
            exists = False
            minValueLength = len(min(dataset[col_name].tolist(), key=len))
            maxValueLength = len(max(dataset[col_name].tolist(), key=len))
            print("Faker method '" + method + "' not found. Resorting to random String")
        # Fitting mode requires each distinct value to have their own translation
        collection = (
            dataset[col_name].unique() if mode == "fitting" else dataset[col_name]
        )
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
            dataset[col_name] = dataset[col_name].map(sensDict)
        else:
            dataset[col_name] = replacementValues
        fake.unique.clear()
        return dataset, sensDict
