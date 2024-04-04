"""Module that handles the full Anonymization pipeline
"""

import sys
import pandas as pd
import modules.visu as vs

from configuration.configurations import DPConfig, SensitiveConfig, ContinuousConfig
from configuration.config_parser import JSONParser
from modules.jdbc_handler import JDBCHandler
from modules.dp_anonymizer import DifferentialPrivacyAnonymizer
from modules.sensitive_anonymizer import SensitiveAnonymizer


def anonymize(
    dataset: pd.DataFrame,
    anon_config: DPConfig,
    cont_config: ContinuousConfig,
    sens_config: SensitiveConfig,
):
    """Method that runs the actual anonymization steps

    Args:
        dataset (pd.DataFrame): The data
        anon_config (DPConfig): Differential privacy config
        cont_config (ContinuousConfig): Continuous columns config
        sens_config (SensitiveConfig): Sensitive data config

    Returns:
        pd.DataFrame: The fully anonymized data
    """
    dp_data = dataset
    if anon_config is not None:
        dp_anonymizer = DifferentialPrivacyAnonymizer(dataset, anon_config, cont_config)
        dp_data = dp_anonymizer.run_anonymization()

    if sens_config is not None:
        sens_anonymizer = SensitiveAnonymizer(dp_data, sens_config)
        dp_data = sens_anonymizer.run_anonymization()

    try:
        vs.generateVisu(dataset, dp_data.copy(deep=True))
    except Exception: # pylint: disable=broad-exception-caught
        print("An exception occurred while trying to visualize the output.")

    return dp_data


def anonymize_db(
    jdbc_handler: JDBCHandler,
    anon_config: DPConfig,
    sens_config: SensitiveConfig,
    cont_config: ContinuousConfig,
):
    """Function that handles all anonymization steps, including pulling and pushing data

    Args:
        jdbc_handler (JDBCHandler): The JDBC connection information
        anon_config (DPConfig): The Differential privacy config
        sens_config (SensitiveConfig): The sensitive data config
        cont_config (ContinuousConfig): The continuous column config
    """
    jdbc_handler.start_jvm()

    conn = jdbc_handler.get_connection()

    table = anon_config.table_name
    dataset, timestamps = jdbc_handler.data_from_table(conn, table)

    dataset_anon = anonymize(dataset, anon_config, cont_config, sens_config)

    # Create empty table
    anon_table_name = jdbc_handler.create_anonymized_table(conn, table)

    # Populate new table
    jdbc_handler.populate_anonymized_table(
        conn, dataset_anon, anon_table_name, timestamps
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

    anonymize_db(jdbc_handler, anon_config, sens_config, cont_config)
    return


if __name__ == "__main__":
    main()
