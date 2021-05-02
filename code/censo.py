import pandas as pd
import numpy as np 
import sys

    
path = 'C://Users/LizianeBarbosa/Desktop/tcc/data/6 - Censo escolar/2017'
regioes = ['CO'] #, 'NORTE', 'NORDESTE', 'SUL', 'SUDESTE'
colunas = ['NU_ANO_CENSO','CO_MUNICIPIO', 'TP_ETAPA_ENSINO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'TP_COR_RACA', 'IN_TRANSPORTE_PUBLICO', 'TP_RESPONSAVEL_TRANSPORTE']
print('Declarados os parametros para ler os dados do Censo')

for regiao in regioes:
    try:
        exec('df_{} = pd.read_csv("{}/MATRICULA_{}.CSV", usecols=colunas, sep = "|")'.format(regiao, path, regiao))
        exec('df_{} = df_{}[(df_{}["TP_ETAPA_ENSINO"] >= 4.0)]'.format(regiao, regiao, regiao))
        exec('df_{} = df_{}[(df_{}["TP_ETAPA_ENSINO"] <= 41.0)]'.format(regiao, regiao, regiao))
        exec('df_{}.drop(["TP_ETAPA_ENSINO"], axis = 1, inplace = True)'.format(regiao))
      
        print('Leitura dos dados do Censo, da Regiao {} realizados com sucesso'.format(regiao))
    
    except Exception as e:
        print('Erro do tipo {} ao ler os dados do Censo da região {}. O erro foi: {}'.format(type(e), regiao, e, sys.exc_info()))


df_CO['count'] = 1

df_count = df_CO["count"].groupby(([df_CO["CO_MUNICIPIO"], df_CO["TP_DEPENDENCIA"], df_CO["TP_LOCALIZACAO"], df_CO["NU_ANO_CENSO"]])).count().to_frame().reset_index()

df_cor_raca = df_CO["TP_COR_RACA"].groupby(([df_CO["CO_MUNICIPIO"], df_CO["TP_DEPENDENCIA"], df_CO["TP_LOCALIZACAO"], df_CO["NU_ANO_CENSO"],df_CO["TP_COR_RACA"]])).count().to_frame().rename(columns = {'TP_COR_RACA': 'qnt_cor_raca'}).reset_index()
df_cor_raca = pd.pivot_table(df_cor_raca, values='qnt_cor_raca',columns=  'TP_COR_RACA', index=['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'NU_ANO_CENSO']).rename(columns = {0: 'cor_raca_nao_declarada', 1: 'cor_raca_branca', 2: 'cor_raca_preta', 3: 'cor_raca_parda', 4: 'cor_raca_amarela', 5: 'cor_raca_indigena'}).reset_index()

df_transp_public = df_CO["IN_TRANSPORTE_PUBLICO"].groupby(([df_CO["CO_MUNICIPIO"], df_CO["TP_DEPENDENCIA"], df_CO["TP_LOCALIZACAO"], df_CO["NU_ANO_CENSO"],df_CO["IN_TRANSPORTE_PUBLICO"]])).count().to_frame().rename(columns = {'IN_TRANSPORTE_PUBLICO': 'qnt_transp_public'})
df_transp_public = pd.pivot_table(df_transp_public, values='qnt_transp_public',columns=  'IN_TRANSPORTE_PUBLICO', index=['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'NU_ANO_CENSO']).rename(columns = {0.0: 'nao_usa_transporte_publico', 1.0: 'usa_transporte_publico'}).reset_index()

df_resp_transp_public = df_CO["TP_RESPONSAVEL_TRANSPORTE"].groupby(([df_CO["CO_MUNICIPIO"], df_CO["TP_DEPENDENCIA"], df_CO["TP_LOCALIZACAO"], df_CO["NU_ANO_CENSO"],df_CO["TP_RESPONSAVEL_TRANSPORTE"]])).count().to_frame().rename(columns = {'TP_RESPONSAVEL_TRANSPORTE': 'qnt_respo_transp'})
df_resp_transp_public = pd.pivot_table(df_resp_transp_public, values='qnt_respo_transp',columns=  'TP_RESPONSAVEL_TRANSPORTE', index=['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'NU_ANO_CENSO']).rename(columns = {1.0: 'transporte_publico_estadual', 2.0: 'transporte_publico_municipal'}).reset_index()

df = df_cor_raca.merge(df_transp_public, 
                       how = 'inner', 
                       on = ['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'NU_ANO_CENSO']).merge(df_resp_transp_public,
                       on = ['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'NU_ANO_CENSO']).merge(df_count,
                       on = ['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'NU_ANO_CENSO'])

df = df.replace(np.nan, 0.0)

df['cor_raca_nao_declarada'] = df['cor_raca_nao_declarada']/df['count']
df['cor_raca_branca'] = df['cor_raca_branca']/df['count']
df['cor_raca_preta'] = df['cor_raca_preta']/df['count']
df['cor_raca_parda'] = df['cor_raca_parda']/df['count']
df['cor_raca_amarela'] = df['cor_raca_amarela']/df['count']
df['cor_raca_indigena'] = df['cor_raca_indigena']/df['count']

df['transporte_publico_estadual'] = df['transporte_publico_estadual']/df['usa_transporte_publico']
df['transporte_publico_municipal'] = df['transporte_publico_municipal']/df['usa_transporte_publico'] 

df['nao_usa_transporte_publico'] = df['nao_usa_transporte_publico']/df['count']
df['usa_transporte_publico'] = df['usa_transporte_publico']/df['count']

df.to_csv(str(path + '/regiao_CO.csv'), sep = ';')
