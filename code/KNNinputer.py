import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.impute import KNNImputer
from scipy.stats import shapiro, anderson, normaltest, spearmanr, variation, jarque_bera


np.seterr(divide='ignore', invalid='ignore')

def knn_inputer(df):
    
    
    df_ = df.isnull().sum().to_frame('N missing values')
    df_ = df_.reset_index()
    df_ = df_.rename(columns = {'index' : 'Tag', 'N missing values' : 'N missing values'})
    df_ = df_.loc[df_['N missing values'] > 0]
    df_ = df_.sort_values(by = 'N missing values', ascending = False)
        
    # Scaler
    scaler = StandardScaler()
    

    #col in df_.select_dtypes(['float64'])
    trans_df_= scaler.fit_transform(df_)
    
    trans_df_ = pd.DataFrame(data = trans_df_, columns = df_.columns)
    
    # Imputer
    imputer = KNNImputer(missing_values = np.nan, n_neighbors=30, weights='uniform')
    trans_df_ = imputer.fit_transform(trans_df_)
    trans_df_ = pd.DataFrame(data = trans_df_, columns = df_.columns)
    
    # Reverter a transformação
    trans_df_ = scaler.inverse_transform(trans_df_)
    trans_df_ = pd.DataFrame(data = trans_df_, columns = df_.columns)
    to_drop = pd.DataFrame(data = df[to_drop])
    to_drop.reset_index(drop=True, inplace=True)
    trans_df_.reset_index(drop=True, inplace=True)
    df = pd.concat([to_drop, trans_df_], axis = 1)
    
    return df