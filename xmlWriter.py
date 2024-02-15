"""Helper Module that converts the arguments of the script 
to an XML file that can be used in benchbase
"""

import xml.etree.ElementTree as ET


def listToString(src: list):
    """Helper function that creates a list from a comma separated string

    Args:
        src (str[]): A list of strings that may be empty

    Returns:
        str: A string
    """
    return "" if src is None else ",".join(src)


def registerElement(root, name: str, value):
    """Method to register a text element under an XML root

    Args:
        root (Element): The root XML element
        name (str): The name of the XML element that should be registered
        value (list | None): The list of values that will be contained in the element
    """

    if value is not None:
        XMLtag = ET.SubElement(root, name)
        XMLtag.text = listToString(value)


def parse(anonConfig: dict, sensConfig: dict, contConfig: dict):
    """Helper function that creates an XML file which can be used in the benchbase config

    Args:
        anonConfig (dict): The Anonymization config dictionary
        sensConfig (dict): The Sensitive config dictionary
        contConfig (dict): The Continuous column config dictionary
    """
    params = ET.Element("parameters")

    ############### Anonymizer Config #################
    anon = ET.SubElement(params, "anonymization")

    anonTable = ET.SubElement(anon, "table")
    anonTable.set("name", anonConfig["table"])
    anonTable.set("epsilon", anonConfig["eps"])
    anonTable.set("pre_epsilon", anonConfig["preEps"])
    anonTable.set("algorithm", anonConfig["alg"])

    registerElement(anonTable, "droppable", anonConfig["hide"])
    registerElement(anonTable, "categorical", anonConfig["cat"])
    registerElement(anonTable, "continuous", anonConfig["cont"])
    registerElement(anonTable, "ordinal", anonConfig["ord"])

    ############### Continuous Config #################
    if contConfig:
        cont = ET.SubElement(anonTable, "continuousConfig")

        for contCol in contConfig["cols"]:
            colName = ET.SubElement(cont, "column")
            colName.set("name", contCol["name"])
            colName.set("bins", contCol["bins"])
            colName.set("lower", contCol["lower"])
            colName.set("upper", contCol["upper"])

    ############### Sensitive Config #################
    if sensConfig:
        sens = ET.SubElement(anonTable, "sensitive")

        for sensCol in sensConfig["cols"]:
            colName = ET.SubElement(sens, "column")
            colName.set("name", sensCol["name"])
            colName.set("method", sensCol["method"])
            colName.set("locales", ",".join(sensCol["locales"]))
            # colName.set("seed", str(sensConfig["seed"])) Implement at later stage

    ET.indent(params)
    byteXML = ET.tostring(params)

    with open("output/benchbaseConfig.xml", "wb") as f:
        f.write(byteXML)


"""
<anonymization>
        <table name="item" epsilon="1.0" pre_epsilon="0.0" algorithm="aim">
            <!-- Column categorization -->
            <droppable>i_id</droppable>
            <categorical>i_name,i_data,i_im_id</categorical>
            <continuous>i_price</continuous>
            <ordinal></ordinal>
            <!-- Continuous column fine-tuning -->
            <continuousConfig>
                <column name="i_price" bins="1000" lower="2.0" upper="100.0"> 
            <continuousConfig>
            <!-- Sensitive value handling -->
            <sensitive>
                <column name="i_name" method="name" locales="en_US" seed="0" />
            </sensitive>
        </table>
    </anonymization>
"""
