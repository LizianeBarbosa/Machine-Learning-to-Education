import pandas as pd
import numpy as np
from scipy.stats import shapiro, normaltest, spearmanr, mannwhitneyu, kruskal, pearsonr


def NormalityTest(df, alpha = 0.05):
    '''
    Esta funcao retorna os resultados dos testes de normalidade para a distribuicao de cada campo existente no dataframe indicado de acordo com o nivel de significancia escolhido
    Parametros:
    df: dataframe original
    alpha: nivel de significancia (default = 0.05)
    Retorno: 
    output: dataframe contendo os resultados dos testes de hipotese para cada variavel numerica
    '''
    input = df.select_dtypes(include = ['float64', 'int64'])
    output_1 = []
    output_2 = []
    # Shapiro Wilk
    x = 0
    for x in range(input.shape[1]):
        stat, p = shapiro(input.iloc[:, x])   
        if p > alpha:
            msg = 'Amostra tem distribuição gaussiana (H0 não foi rejeitada)'
        else:
            msg = 'Amostra não possui distribuição gaussiana (Rejeita-se H0)'
        output_1.append([input.iloc[:, x].name, msg])
    output_1 = pd.DataFrame(output_1, columns = ['Variável', 'Shapiro Wilk Result'])
    # D'Agostino-Pearson
    x = 0
    for x in range(input.shape[1]):
        stat, p = normaltest(input.iloc[:, x])   
        if p > alpha:
            msg = 'Amostra tem distribuição gaussiana (H0 não foi rejeitada)'
        else:
            msg = 'Amostra não possui distribuição gaussiana (Rejeita-se H0)'
        output_2.append([input.iloc[:, x].name, msg])
    output_2 = pd.DataFrame(output_2, columns = ['Variável', 'D Agostinos K2 Result'])
    output = output_1.merge(output_2, on='Variável', how='inner')
    return output
