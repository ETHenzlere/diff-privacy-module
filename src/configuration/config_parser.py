import json

from configuration.configurations import (
    DPConfig,
    DPColumnConfig,
    SensitiveConfig,
    SensitiveEntry,
    ContinuousConfig,
    ContinuousEntry,
)
from modules.jdbc_handler import JDBCHandler

class JSONParser:

    def __init__(self, args: list[str]):
        self.args = args

    def get_jdbc_config(self):
        dict_object = json.loads(self.args[1])
        return JDBCHandler(
            dict_object["driver"],
            dict_object["url"],
            dict_object["username"],
            dict_object["password"],
            dict_object["jarPath"],
        )

    def get_anon_config(self):
        dict_object = json.loads(self.args[2])
        col_config = DPColumnConfig(
            dict_object["hide"],
            dict_object["cat"],
            dict_object["cont"],
            dict_object["ord"]
        )

        return DPConfig(
            dict_object["table"],
            dict_object["eps"],
            dict_object["preEps"],
            dict_object["alg"],
            col_config
        )

    def get_sens_config(self):
        dict_object = json.loads(self.args[3])
        sens_column_list = []

        for entry in dict_object:
            sens_column_list.append(
                SensitiveEntry(
                    entry["name"], entry["method"],entry["mode"], entry["locales"], entry["seed"]
                )
            )

        return SensitiveConfig(sens_column_list)

    def get_cont_config(self):
        dict_object = json.loads(self.args[4])
        cont_column_list = []

        for entry in dict_object:
            cont_column_list.append(
                ContinuousEntry(
                    entry["name"], entry["bins"], entry["lower"], entry["upper"]
                )
            )

        return ContinuousConfig(cont_column_list)
