import jaydebeapi
import json
import sys
import jpype


def columnsFromTable(curs, table):
    getTableQuery = "SELECT * FROM {0} LIMIT 1".format(table)
    curs.execute(getTableQuery)
    curs.fetchall()
    meta = curs.description

    cols = []
    for entry in meta:
        cols.append(str(entry[0]))

    return cols


def getMeta(table, jdbcConfig: dict):
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
    if len(sys.argv) < 3:
        raise Exception("Not enough arguments provided: <tableName> <jdbcConfig>")

    name = sys.argv[1]
    jdbcConfig = json.loads(sys.argv[2])

    columns = getMeta(name, jdbcConfig)

    print(columns)
    return


if __name__ == "__main__":
    main()
