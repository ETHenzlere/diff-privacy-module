"""Helper Module that converts the arguments of the script 
to an XML file that can be used in benchbase
"""
import xml.etree.ElementTree as ET


def parse(anonConfig: dict, sensConfig: dict):
    """Helper function that creates an XML file which can be used in the benchbase config

    Args:
        anonConfig (dict): The Anonymization config dictionary
        sensConfig (dict): The Sensitive config dictionary
    """
    params = ET.Element("parameters")

    ############### Anonymizer Config #################
    anon = ET.SubElement(params, "anon")
    anonTable = ET.SubElement(anon, "anonTable")
    tablename = ET.SubElement(anonTable, "tablename")
    droppable = ET.SubElement(anonTable, "droppable")
    categorical = ET.SubElement(anonTable, "categorical")
    continuous = ET.SubElement(anonTable, "continuous")
    ordinal = ET.SubElement(anonTable, "ordinal")
    eps = ET.SubElement(anonTable, "eps")
    preEps = ET.SubElement(anonTable, "preEps")
    algorithm = ET.SubElement(anonTable, "algorithm")

    tablename.text = anonConfig["table"]
    droppable.text = ",".join(anonConfig["hide"])
    categorical.text = ",".join(anonConfig["cat"])
    continuous.text = ",".join(anonConfig["cont"])
    ordinal.text = ",".join(anonConfig["ord"])
    eps.text = anonConfig["eps"]
    preEps.text = anonConfig["preEps"]
    algorithm.text = anonConfig["alg"]

    ############### Sensitive Config #################
    sens = ET.SubElement(params, "sensitive")
    sens.set("seed", str(sensConfig["seed"]))

    for sensCol in sensConfig["cols"]:
        colName = ET.SubElement(sens, "colName")
        colName.text = sensCol["name"]
        colName.set("method", sensCol["method"])
        colName.set("locales", ",".join(sensCol["locales"]))

    ET.indent(params)
    byteXML = ET.tostring(params)

    with open("output/benchbaseConfig.xml", "wb") as f:
        f.write(byteXML)
