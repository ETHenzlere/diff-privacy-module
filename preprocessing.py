from snsynth.transform import *

def getTransformer(dataset,algorithm,categorical,continuous,ordinal,continuousConfig):
    """Method that returns a handcrafted transformer for differential privacy algorithms

    Args:
        dataset (DataFrame): A pandas dataframe

    Returns:
        tt (TableTransformer): The TableTransformer object
    """

    transformerStyle = 'cube'
    if("gan" in algorithm):
        transformerStyle = 'gan'

    tt = TableTransformer.create(
        dataset,
        categorical_columns=categorical,
        continuous_columns=continuous,
        ordinal_columns=ordinal,
        style=transformerStyle,
        constraints=getConstraints(continuousConfig)
    )
    print("Instantiated Transformer")
    return tt

def getConstraints(objects):
    constraints = {}
   
    for colEntry in objects:
        colName = colEntry["name"]
        bins = int(colEntry["bins"])

        lower = colEntry["lower"]
        upper = colEntry["upper"]

        minB = None
        maxB = None

        if(lower):
            if('.' in lower):
                minB = float(lower)
            else:
                minB = int(lower)
        if(upper):
            if('.' in upper):
                maxB = float(upper)
            else:
                maxB = int(upper)

        constraints[colName] = BinTransformer(bins=bins,lower=minB,upper=maxB)

    return constraints
    
    