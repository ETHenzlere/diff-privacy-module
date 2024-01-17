"""Module that is used by the application to gather metadata from JDBC connections
"""
import json
import sys
import jaydebeapi
import jpype

def columnsFromTable(curs, table):
    """A helper function that returns a list of column names

    Args:
        curs (any): JDBC connection cursor
        table (str): Name of the table

    Returns:
        str[]: A list of column names
    """
    getTableQuery = f"SELECT * FROM {table} LIMIT 1"
    curs.execute(getTableQuery)
    curs.fetchall()
    meta = curs.description

    cols = []
    for entry in meta:
        cols.append(str(entry[0]))

    return cols


def getMeta(table, jdbcConfig: dict):
    """Method that returns a list of columns corresponding to a table

    Args:
        table (str): Name of the table
        jdbcConfig (dict): JSON dictionary containing connection information

    Returns:
        cols (str[]): List of columns of the defined table
    """
    driver = jdbcConfig["driver"]
    url = jdbcConfig["url"]
    username = jdbcConfig["username"]
    password = jdbcConfig["password"]
    jar = jdbcConfig["jarPath"]

    jpype.startJVM(classpath=[jar])

    conn = jaydebeapi.connect(driver, url, [username, password])

    curs = conn.cursor()

    cols = columnsFromTable(curs, table)
    curs.close()
    conn.close()
    return cols


def main():
    """Main function that handles sys args

    Raises:
        Exception: Not enough arguments provided
    """
    if len(sys.argv) < 3:
        raise RuntimeError("Not enough arguments provided: <tableName> <jdbcConfig>")

    name = sys.argv[1]
    jdbcConfig = json.loads(sys.argv[2])

    columns = getMeta(name, jdbcConfig)

    print(columns)

    return


if __name__ == "__main__":
    main()
