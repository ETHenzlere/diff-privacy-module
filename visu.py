"""A helper module that generates different JSON files and images
from an original and anonymized DataFrame
"""
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

DESCRIBE_ROWS = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]


def compareNumericalCols(dataset, synthFrame, continuous):
    jsonDict = []
    numPlots = len(continuous)

    if numPlots < 1:
        print("No numerical columns found!")
    else:
        for _, col in enumerate(continuous):
            pltO = sns.kdeplot(data=dataset[col])
            pltA = sns.kdeplot(data=synthFrame[col])

            x, y = pltO.get_lines()[0].get_data()

            xA, yA = pltA.get_lines()[1].get_data()
            jsonObj = {
                "column": col,
                "origX": list(x),
                "origY": list(y),
                "anonX": list(xA),
                "anonY": list(yA),
            }
            jsonDict.append(jsonObj)
            plt.clf()

    with open("output/kdeInfo.json", "w", encoding="utf-8") as outfile:
        json.dump(jsonDict, outfile)

    boxNumerical(dataset, synthFrame, continuous)


def boxNumerical(dataset, synthFrame, continuous):
    jsonDict = []
    rows = len(continuous)

    if rows > 0:
        for column in continuous:
            colOrig = dataset[column]
            colAnon = synthFrame[column]
            dscO = list(colOrig.describe())
            dscA = list(colAnon.describe())
            celltext = []
            for j, origVal in enumerate(dscO):
                celltext.append(
                    [
                        round(origVal, 2),
                        round(dscA[j], 2),
                        abs(round(origVal - dscA[j], 2)),
                    ]
                )
            jsonObj = {"column": column, "data": celltext}
            jsonDict.append(jsonObj)
    with open("output/numInfo.json", "w", encoding="utf-8") as outfile:
        json.dump(jsonDict, outfile)


def compareCategorical(dataset, synthFrame, categorical):
    jsonDict = []

    for col in categorical:
        catOrig = dataset[col].value_counts().reset_index()
        catAnon = synthFrame[col].value_counts().reset_index()

        catOrig.columns = [col, "original"]
        catAnon.columns = [col, "anonymized"]

        combined = catOrig.merge(catAnon, how="left", on=col)
        combined[col] = combined[col].astype(str)
        jsonObj = {
            "column": col,
            "distinct": list(combined[col]),
            "original": list(combined["original"]),
            "anonymized": list(combined["anonymized"]),
        }
        jsonDict.append(jsonObj)
    with open("output/catInfo.json", "w", encoding="utf-8") as outfile:
        json.dump(jsonDict, outfile)


def correlationMap(orig, anon):
    # Make plot
    sns.set(font_scale=1.5)
    fig, axes = plt.subplots(1, 2, figsize=(24, (12)))

    # Annot = True for annotation
    axes[0].set_title("Original")
    sns.heatmap(
        orig,
        annot=False,
        cbar=True,
        cmap=sns.cubehelix_palette(5),
        square=True,
        vmax=1,
        vmin=0,
        linewidths=0.5,
        ax=axes[0],
    )
    axes[1].set_title("Anonymized")
    sns.heatmap(
        anon,
        annot=False,
        cbar=True,
        cmap=sns.cubehelix_palette(5),
        square=True,
        vmax=1,
        vmin=0,
        linewidths=0.5,
        ax=axes[1],
    )
    plt.tight_layout()
    fig.savefig("output/corrMap.png")
    plt.close()


def generateVisu(continuous, categorical, ordinal, dataset, synthFrame):
    continuous = list(dataset.select_dtypes(include=[np.number]))
    categorical = list(dataset.select_dtypes(exclude=[np.number]))

    compareCategorical(dataset, synthFrame, categorical)
    compareNumericalCols(dataset, synthFrame, continuous)

    dataset[categorical] = dataset[categorical].apply(lambda x: pd.factorize(x)[0])
    synthFrame[categorical] = synthFrame[categorical].apply(
        lambda x: pd.factorize(x)[0]
    )

    corrOrig = abs(dataset).corr()
    corrAnon = abs(synthFrame).corr()

    correlationMap(corrOrig, corrAnon)
