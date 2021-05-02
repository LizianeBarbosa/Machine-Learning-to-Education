import requests as rq
import io
import pandas as pd
from zipfile import ZipFile
import json
import numpy as np


class GetData():
    '''
    Classe para aquisição de dados de diferentes pesquisas do INEP.
    '''

    def __init__(self, config_path):
        '''
        ===================================
        Método construtor da classe GetData
        ===================================

        Parâmetros
        ----------
        config_path : str
            Caminho para arquivo json contendo as url's para requisição e aquisição dos dados        
        '''
        self.config = json.load(open(config_path, 'rb'))

    def get_inse(self):
        '''
        Requisita e adquire os dados de INSE do INEP

        Retornos
        --------
        df pandas.core.frame.DataFrame
            Dataframe do pandas contendo os dados empilhados do INSE
        
        '''
        try:
            urls_inse = self.config["inse"]            

            for url in urls_inse:
                
                ano = url.split("/")[5]

                req = rq.get(url, allow_redirects = True)
                zip = ZipFile(io.BytesIO(req.content))

                file = zip.read(zip.namelist()[0])


                exec("df_inse_{} = pd.read_excel(io.BytesIO({}))".format(ano, file), globals())
                exec("df_inse_{}_colunas = df_inse_{}.loc[8]".format(ano, ano), globals())
                exec("df_inse_{} = df_inse_{}.loc[9:]".format(ano,ano), globals())
                exec("df_inse_{}.columns = df_inse_{}_colunas".format(ano, ano), globals())
            
            
            df_inse_2015.drop(['CO_ESCOLA', 'NOME_ESCOLA', 'CO_UF', 'NOME_UF', 'NOME_MUNICIPIO', 'ID_AREA'], axis = 1, inplace = True)
            df_2015 = df_inse_2015.groupby(['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO']).apply(lambda dfx: (dfx["QTD_ALUNOS_INSE"] * dfx["INSE_VALOR_ABSOLUTO"]).sum() / dfx["QTD_ALUNOS_INSE"].sum()).to_frame().reset_index().rename(columns = {0: "inse"})
            df_2015['ano'] = 2015

            df_inse_2011_2013.drop(['COD_ESCOLA', 'NOME_ESCOLA', 'COD_ESTADO', 'NOME_ESTADO', 'NOME_MUNICIPIO', 'AREA', 'ID_AREA', 'REDE', 'LOCALIZACAO'], axis = 1, inplace = True)
            df_2011_2013 = df_inse_2011_2013.groupby(['COD_MUNICIPIO', 'ID_REDE', 'ID_LOCALIZACAO']).apply(lambda dfx: (dfx["QTD_ALUNOS_INSE"] * dfx["INSE - VALOR ABSOLUTO"]).sum() / dfx["QTD_ALUNOS_INSE"].sum()).to_frame().reset_index().rename(columns = {'INSE - VALOR ABSOLUTO': 'INSE_VALOR_ABSOLUTO', 'COD_MUNICIPIO': 'CO_MUNICIPIO','ID_REDE' : 'TP_DEPENDENCIA', 'ID_LOCALIZACAO': 'TP_LOCALIZACAO', 0: "inse"})            
            df_2011_2013['ano'] = 2013


            df = df_2015.append(df_2011_2013)
            df = df.rename(columns = {'TP_DEPENDENCIA' : 'dep_adm', 'TP_LOCALIZACAO':'locallizacao', 'CO_MUNICIPIO': 'cod_ibge'})

            df['ano'] = df['ano'].astype(object)
            df['cod_ibge'] = df['cod_ibge'].astype(object)
            df['dep_adm'] = df['dep_adm'].astype(object)
            df['locallizacao'] = df['locallizacao'].astype(object)

            df['inse'] = df['inse'].astype(float)

            print("Dados do INSE adquiridos com sucesso")        
            return df
        
        except Exception as e:
            print("Ocorreu um erro durante a execução do método get_inse(), do tipo {}: {}".format(type(e), e, exc_info = True))

    def get_ideb(self):
        '''
        Requisita e adquire os dados de IDEB do INEP

        Retornos
        --------
        df pandas.core.frame.DataFrame
            Dataframe do pandas contendo os dados empilhados do IDEB
        
        '''

        urls_ideb = self.config["ideb"]

        try:
            
            
            for url in urls_ideb:
                
                etapa_ensino = url.split("/")[7].split("_")[2]
                
                req = rq.get(url, allow_redirects = True)
                zip = ZipFile(io.BytesIO(req.content))

                file = zip.read(zip.namelist()[1])


                exec("anos_{} = pd.read_excel(io.BytesIO({}))".format(etapa_ensino, file), globals())
                exec("anos_{}_colunas = anos_{}.loc[8]".format(etapa_ensino, etapa_ensino), globals())
                exec("anos_{} = anos_{}.loc[9:]".format(etapa_ensino,etapa_ensino), globals())
                exec("anos_{}.columns = anos_{}_colunas".format(etapa_ensino,etapa_ensino), globals())            
            
            df = anos_finais.merge(anos_iniciais, how = 'inner', on = ['SG_UF', 'COD_MUN', 'NO_MUNICIPIO', 'REDE'])
            
            df = df[~(df['REDE'] == 'Pública')]
            
            df = df[['COD_MUN', 'REDE', 'PAD14_13','PAD58_13', 'PAD14_15', 'PAD58_15','PAD14_17','PAD58_17']]
            df.replace('-', np.nan, inplace = True)

            df['a2013'] = (df['PAD14_13'] + df['PAD58_13'])/2
            df['a2014'] = df['a2013']
            
            df['a2015'] = (df['PAD14_15'] + df['PAD58_15'])/2
            df['a2016'] = df['a2015']

            df['a2017'] = (df['PAD14_17'] + df['PAD58_17'])/2
            df['a2018'] = df['a2017']

            df = df[['COD_MUN', 'REDE', 'a2013', 'a2014', 'a2015', 'a2016', 'a2017', 'a2018']]
            df = df[~(df['COD_MUN'] == np.nan)]

            df = pd.melt(df, id_vars = ['COD_MUN', 'REDE'], 
                             value_name = 'nota_padronizada',
                             var_name = 'ano')

            df['ano'].replace('a2013', 2013, inplace = True)
            df['ano'].replace('a2014', 2014, inplace = True)
            df['ano'].replace('a2015', 2015, inplace = True)
            df['ano'].replace('a2016', 2016, inplace = True)
            df['ano'].replace('a2017', 2017, inplace = True)
            df['ano'].replace('a2018', 2018, inplace = True)
            
            df = df.rename(columns = {'COD_MUN': 'cod_ibge', 'REDE': 'dep_adm'})
            
            df['dep_adm'] = df['dep_adm'].replace(['Federal'], 1)
            df['dep_adm'] = df['dep_adm'].replace(['Estadual'], 2)
            df['dep_adm'] = df['dep_adm'].replace(['Municipal'], 3)
            df['dep_adm'] = df['dep_adm'].replace(['Privada'], 4)

            df['ano'] = df['ano'].astype(object)
            df['cod_ibge'] = df['cod_ibge'].astype(object)
            df['dep_adm'] = df['dep_adm'].astype(object)

            df['nota_padronizada'] = df['nota_padronizada'].astype(float)

            print("Dados do IDEB adquiridos com sucesso")        
            return df
        
        except Exception as e:
            print("Ocorreu um erro durante a execução do método get_ideb(), do tipo {}: {}".format(type(e), e, exc_info = True))
        
    def get_icg(self):
        '''
        Requisita e adquire os dados de ICG do INEP

        Retornos
        --------
        df pandas.core.frame.DataFrame
            Dataframe do pandas contendo os dados empilhados do ICG
        
        '''

        urls_icg = self.config["icg"]

        try:
                        
            for url in urls_icg:
                
                ano = url.split("/")[5]
                req = rq.get(url, allow_redirects = True)
                zip = ZipFile(io.BytesIO(req.content))

                if int(ano)>=2015 and int(ano)<=2018:
                    file = zip.read(zip.namelist()[2])
                
                else:
                    file = zip.read(zip.namelist()[0])


                exec("icg_{} = pd.read_excel(io.BytesIO({}))".format(ano, file), globals())
                exec("icg_{}_colunas = icg_{}.loc[7]".format(ano, ano), globals())
                exec("icg_{} = icg_{}.loc[8:]".format(ano, ano), globals())
                exec("icg_{}.columns = icg_{}_colunas".format(ano, ano), globals())            
            
            icg_2015 = icg_2015.rename(columns={'Dependad': 'TIPOLOCA', 'TIPOLOCA': 'Dependad'})
            df = icg_2013.copy()

            for numero in range(2014,2019):
                exec('df = df.append(icg_{})'.format(numero))
            
            # removendo linhas de totais
            df = df[~(df['TIPOLOCA'] == 'Total')]
            df = df[~(df['Dependad'] == 'Total')]
            df = df[~(df['Dependad'] == 'Pública')]
            df = df[~(df['Dependad'] == 'Público')]

            df['Dependad'] = df['Dependad'].replace(['Federal'], 1)
            df['Dependad'] = df['Dependad'].replace(['Estadual'], 2)
            df['Dependad'] = df['Dependad'].replace(['Municipal'], 3)
            df['Dependad'] = df['Dependad'].replace(['Privada'], 4)
            df['TIPOLOCA'] = df['TIPOLOCA'].replace(['Urbana'], 1)
            df['TIPOLOCA'] = df['TIPOLOCA'].replace(['Rural'], 2)
            
            df = df.rename(columns={'Ano':'ano', 'Dependad': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'PK_COD_MUNICIPIO': 'cod_ibge'})    
            df = df[~(df['cod_ibge'].isnull() == True)]
            df = df[['ano', 'cod_ibge', 'dep_adm', 'locallizacao', 'ICG_1', 'ICG_2', 'ICG_3', 'ICG_4', 'ICG_5', 'ICG_6']]
            
            df['ano'] = df['ano'].astype(object)
            df['cod_ibge'] = df['cod_ibge'].astype(object)
            df['dep_adm'] = df['dep_adm'].astype(object)
            df['locallizacao'] = df['locallizacao'].astype(object)

            df['ICG_1'] = df['ICG_1'].astype(float)     
            df['ICG_2'] = df['ICG_2'].astype(float)     
            df['ICG_3'] = df['ICG_3'].astype(float)     
            df['ICG_4'] = df['ICG_4'].astype(float)     
            df['ICG_5'] = df['ICG_5'].astype(float)     
            df['ICG_6'] = df['ICG_6'].astype(float)     


            print("Dados do ICG adquiridos com sucesso")        
            return df
        
        except Exception as e:
            print("Ocorreu um erro durante a execução do método get_icg(), do tipo {}: {}".format(type(e), e, exc_info = True))

    def get_taxas_rendimento(self):
        '''
        Requisita e adquire os dados das taxas de rendimento do INEP

        Retornos
        --------
        df pandas.core.frame.DataFrame
            Dataframe do pandas contendo os dados empilhados das taxas de rendimento
        
        '''

        urls_taxas_rendimento = self.config["taxas_rendimento"]
        colunas_taxas_rendimento = self.config["colunas_taxas_rendimento"]
        
        try:
                        
            for url in urls_taxas_rendimento:
                
                ano = url.split("/")[5]
                req = rq.get(url, allow_redirects = True)
                zip = ZipFile(io.BytesIO(req.content))

                if int(ano)<=2014:
                    file = zip.read(zip.namelist()[0])
                  
                elif int(ano)>=2016 and int(ano)<=2017:
                    file = zip.read(zip.namelist()[4])
                
                else:
                    file = zip.read(zip.namelist()[3])


                exec("taxas_rendimento_{} = pd.read_excel(io.BytesIO({}))".format(ano, file), globals())
                exec("taxas_rendimento_{} = taxas_rendimento_{}.loc[8:]".format(ano, ano), globals())
                exec("taxas_rendimento_{}.columns = {}".format(ano, colunas_taxas_rendimento), globals())            

            df = taxas_rendimento_2013.copy()

            for numero in range(2014,2019):
                exec('df = df.append(taxas_rendimento_{})'.format(numero))

            # removendo linhas de totais e colunas drop
            df.drop(df.filter(regex = "drop"), axis = 1, inplace = True)
            df = df[~(df['locallizacao'] == 'Total')]
            df = df[~(df['dep_adm'] == 'Total')]
            df = df[~(df['dep_adm'] == 'Publico')]
            df = df[~(df['dep_adm'] == 'Pública')]

            df = df.replace('--', np.nan)


            df['dep_adm'] = df['dep_adm'].replace(['Federal'], 1)
            df['dep_adm'] = df['dep_adm'].replace(['Estadual'], 2)
            df['dep_adm'] = df['dep_adm'].replace(['Municipal'], 3)
            df['dep_adm'] = df['dep_adm'].replace(['Privada'], 4)
            df['dep_adm'] = df['dep_adm'].replace(['Particular'], 4)

            df['locallizacao'] = df['locallizacao'].replace(['Urbana'], 1)
            df['locallizacao'] = df['locallizacao'].replace(['Rural'], 2)
            df = df[~(df['cod_ibge'].isnull() == True)]

            df.drop(['regiao', 'uf', 'nome_munic'], axis = 1, inplace = True)
            
            df['ano'] = df['ano'].astype(object)
            df['cod_ibge'] = df['cod_ibge'].astype(object)
            df['dep_adm'] = df['dep_adm'].astype(object)
            df['locallizacao'] = df['locallizacao'].astype(object)

            df['tot_fund_tx_ap'] = df['tot_fund_tx_ap'].astype(float)
            df['anos_iniciais_tx_ap'] = df['anos_iniciais_tx_ap'].astype(float)
            df['anos_finais_tx_ap'] = df['anos_finais_tx_ap'].astype(float)
            df['tot_fund_tx_rep'] = df['tot_fund_tx_rep'].astype(float)
            df['anos_iniciais_tx_rep'] = df['anos_iniciais_tx_rep'].astype(float)
            df['anos_finais_tx_rep'] = df['anos_finais_tx_rep'].astype(float)
            df['tot_fund_tx_aban'] = df['tot_fund_tx_aban'].astype(float)
            df['anos_iniciais_tx_aban'] = df['anos_iniciais_tx_aban'].astype(float)
            df['anos_finais_tx_aban'] = df['anos_finais_tx_aban'].astype(float)

            print("Dados das taxas de rendimento adquiridos com sucesso")        
            return df
        
        except Exception as e:
            print("Ocorreu um erro durante a execução do método get_taxas_rendimento(), do tipo {}: {}".format(type(e), e, exc_info = True))

    def get_taxas_distorcao(self):
        '''
        Requisita e adquire os dados das taxas de distorcao do INEP

        Retornos
        --------
        df pandas.core.frame.DataFrame
            Dataframe do pandas contendo os dados empilhados da taxa de distorção
        
        '''

        urls_taxas_distorcao = self.config["taxas_distorcao"]
        colunas_taxas_distorcao_13_14 = self.config["colunas_taxas_distorcao_13_14"]
        colunas_taxas_distorcao_15 = self.config["colunas_taxas_distorcao_15"]
        colunas_taxas_distorcao_16_17_18 = self.config["colunas_taxas_distorcao_16_17_18"]

        try:
                        
            for url in urls_taxas_distorcao:
                
                ano = url.split("/")[5]
                req = rq.get(url, allow_redirects = True)
                zip = ZipFile(io.BytesIO(req.content))

                if int(ano)<=2014:
                    file = zip.read(zip.namelist()[0])
                                  
                else:
                    file = zip.read(zip.namelist()[3])


                exec("taxas_distorcao_{} = pd.read_excel(io.BytesIO({}))".format(ano, file), globals())
                exec("taxas_distorcao_{} = taxas_distorcao_{}.loc[8:]".format(ano, ano), globals())
                
                if (ano <= 2014):
                    exec("taxas_distorcao_{}.columns = {}".format(ano, colunas_taxas_distorcao_13_14), globals())            
                
                elif (ano >= 2016):
                    exec("taxas_distorcao_{}.columns = {}".format(ano, colunas_taxas_distorcao_16_17_18), globals())            
                
                else:
                    exec("taxas_distorcao_{}.columns = {}".format(ano, colunas_taxas_distorcao_15), globals())            
            
                taxas_distorcao_2013 = taxas_distorcao_2013.rename(columns={'Dependad': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'PK_COD_MUNICIPIO': 'cod_ibge' })    
                taxas_distorcao_2014 = taxas_distorcao_2014.rename(columns={'Dependad': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'PK_COD_MUNICIPIO': 'cod_ibge' })    
                taxas_distorcao_2015 = taxas_distorcao_2015.rename(columns={'NU_ANO_CENSO':'ano','DEPENDAD': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'CO_MUNICIPIO': 'cod_ibge' })    
                taxas_distorcao_2016 = taxas_distorcao_2016.rename(columns={'NU_ANO_CENSO':'ano','DEPENDAD': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'CO_MUNICIPIO': 'cod_ibge' })    
                taxas_distorcao_2017 = taxas_distorcao_2017.rename(columns={'NU_ANO_CENSO':'ano','DEPENDAD': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'CO_MUNICIPIO': 'cod_ibge' })    
                taxas_distorcao_2018 = taxas_distorcao_2018.rename(columns={'NU_ANO_CENSO':'ano','DEPENDAD': 'dep_adm', 'TIPOLOCA': 'locallizacao', 'CO_MUNICIPIO': 'cod_ibge' })    

            df = taxas_distorcao_2013.copy()

            for numero in range(2014,2019):
                exec('df = df.append(taxas_distorcao_{})'.format(numero))

            # removendo linhas de totais
            df = df[~(df['locallizacao'] == 'Total')]
            df = df[~(df['dep_adm'] == 'Total')]
            df = df[~(df['dep_adm'] == 'Pública')]
            df = df[~(df['dep_adm'] == 'Público')]
            df = df[~(df['dep_adm'] == 'Publico')]

            df['dep_adm'] = df['dep_adm'].replace(['Federal'], 1)
            df['dep_adm'] = df['dep_adm'].replace(['Estadual'], 2)
            df['dep_adm'] = df['dep_adm'].replace(['Municipal'], 3)
            df['dep_adm'] = df['dep_adm'].replace(['Privada'], 4)
            df['locallizacao'] = df['locallizacao'].replace(['Urbana'], 1)
            df['locallizacao'] = df['locallizacao'].replace(['Rural'], 2)
            df = df.replace('--', np.nan)
           
            df = df[~(df['cod_ibge'].isnull() == True)]
            df = df[['ano', 'cod_ibge', 'locallizacao', 'dep_adm', 'TDI_FUN', 'TDI_F14', 'TDI_F58']]
            
            
            df['ano'] = df['ano'].astype(object)
            df['cod_ibge'] = df['cod_ibge'].astype(object)
            df['dep_adm'] = df['dep_adm'].astype(object)
            df['locallizacao'] = df['locallizacao'].astype(object)

            df['TDI_FUN'] = df['TDI_FUN'].astype(float)
            df['TDI_F14'] = df['TDI_F14'].astype(float)
            df['TDI_F58'] = df['TDI_F58'].astype(float)

            print("Dados da taxa de distorção adquiridos com sucesso")        
            return df
        
        except Exception as e:
            print("Ocorreu um erro durante a execução do método get_taxas_distorcao(), do tipo {}: {}".format(type(e), e, exc_info = True))

