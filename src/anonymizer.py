"""Module that handles the full Anonymization pipeline
"""

import sys
import pandas as pd
import modules.visu as vs

from configuration.configurations import DPConfig, SensitiveConfig, ContinuousConfig
from modules.jdbc_handler import JDBCHandler
from configuration.config_parser import JSONParser
from modules.dp_anonymizer import DifferentialPrivacyAnonymizer
from modules.sensitive_anonymizer import SensitiveAnonymizer


"""Module that handles the full Anonymization pipeline
"""

def anonymize(
    dataset: pd.DataFrame,
    anon_config: DPConfig,
    cont_config: ContinuousConfig,
    sens_config: SensitiveConfig,
):
    dp_data = dataset
    if anon_config is not None:
        dp_anonymizer = DifferentialPrivacyAnonymizer(dataset, anon_config, cont_config)
        dp_data = dp_anonymizer.run_anonymization()

    if sens_config is not None:
        sens_anonymizer = SensitiveAnonymizer(dp_data,sens_config)
        dp_data = sens_anonymizer.run_anonymization()

    try:
        vs.generateVisu(dataset, dp_data.copy(deep=True))
    except:
        print("An exception occurred while trying to visualize")

    return dp_data


def anonymizeDB(
    jdbc_handler: JDBCHandler,
    anon_config: DPConfig,
    sensConfig: SensitiveConfig,
    contConfig: ContinuousConfig,
):
    jdbc_handler.start_jvm()

    conn = jdbc_handler.get_connection()

    table = anon_config.table_name
    dataset, timestamps = jdbc_handler.data_from_table(conn, table)

    datasetAnon = anonymize(
        dataset, anon_config, contConfig, sensConfig)

    # Create empty table
    anon_table_name = jdbc_handler.create_anonymized_table(conn, table)

    # Populate new table
    jdbc_handler.populate_anonymized_table(
        conn, datasetAnon, anon_table_name, timestamps
    )

    conn.close()


def main():
    """Entry method"""
    if len(sys.argv) < 5:
        print(
            "Not enough arguments provided: <jdbc_config> <anon_config> <sens_config> <cont_config>"
        )
        return
    
    config_parser = JSONParser(sys.argv)

    jdbc_handler = config_parser.get_jdbc_config()
    anon_config = config_parser.get_anon_config()
    sens_config = config_parser.get_sens_config()
    cont_config = config_parser.get_cont_config()

    anonymizeDB(jdbc_handler, anon_config, sens_config, cont_config)
    return


if __name__ == "__main__":
    main()
