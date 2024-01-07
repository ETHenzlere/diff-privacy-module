from snsynth.transform import *

def getTransformer(dataset):
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