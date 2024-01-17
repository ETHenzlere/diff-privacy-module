from snsynth.transform import *

def getTransformer(dataset):
    """Method that returns a handcrafted transformer for differential privacy algorithms

    Args:
        dataset (DataFrame): A pandas dataframe

    Returns:
        tt (TableTransformer): The TableTransformer object
    """

    tt = TableTransformer.create(
        dataset,
        style='cube',
        constraints={
            'name':
                ChainTransformer([
                    LabelTransformer(False),
                ]),
            'email':
                ChainTransformer([
                    LabelTransformer(False),
                ])
        }
    )
    return tt