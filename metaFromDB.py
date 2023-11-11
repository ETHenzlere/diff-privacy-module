import jaydebeapi
from bs4 import BeautifulSoup
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


def getMeta(table):
    # Reading the data inside the xml file to a variable under the name  data
    with open("config.xml", "r") as f:
        data = f.read()

    # Passing the stored data inside the beautifulsoup parser
    xmlConfig = BeautifulSoup(data, "xml")

    driver = xmlConfig.find("driver").text
    url = xmlConfig.find("url").text
    username = xmlConfig.find("username").text
    password = xmlConfig.find("password").text
    jar = xmlConfig.find("jarPath").text

    jpype.startJVM(classpath=[jar])

    conn = jaydebeapi.connect(driver, url, [username, password])

    curs = conn.cursor()

    cols = columnsFromTable(curs, table)
    curs.close()
    conn.close()
    return cols


def main():
    if len(sys.argv) < 2:
        raise Exception("Not enough arguments provided: <tableName>")
    name = sys.argv[1]

    columns = getMeta(name)

    print(columns)
    return


if __name__ == "__main__":
    main()
