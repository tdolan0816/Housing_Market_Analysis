import pandas as pd
import pyodbc as db
import sqlalchemy as sa
from sqlalchemy import create_engine

input_file_hs = 'C:\housing_market_analysis\AVG_HS_2009_2022.csv'
input_file_const = r'C:\housing_market_analysis\NCONST_2009_2022.csv'
input_file_pop = r'C:\housing_market_analysis\POP_2009_2023.csv'

# Read the CSV file into a DataFrame object
df_hs = pd.read_csv(input_file_hs, header=0)
df_const_pivot = pd.read_csv(input_file_const, header=0)
df_pop = pd.read_csv(input_file_pop, header=0)

# Melt the DataFrame object to create a new DataFrame object, 
# id_vars is the column(s) that will be kept as identifier variables, 
# var_name is the name of the new column that will be created with the column headers of the original DataFrame object, 
# value_name is the name of the new column that will be created with the values of the original DataFrame object
df_hs_pivot = df_hs.melt(id_vars=['ST_NAME_AVG_HS', 'ST_ABV_AVG_HS', 'REGION_AVG_HS'], var_name='YEAR', value_name='AVG_HS_PRICE').sort_values(by=['ST_NAME_AVG_HS', 'YEAR'], ascending=[True, True])
df_pop_pivot = df_pop.melt(id_vars=['ST_NAME_POP_EST', 'ST_ABV_POP_EST', 'REGION_POP_EST'], var_name='YEAR', value_name='POP_AVG').sort_values(by=['ST_NAME_POP_EST', 'YEAR'], ascending=[True, True])
df_const_pivot = df_const_pivot.melt(id_vars=['REGION_AVG_CONST'], var_name='YEAR', value_name='NCONST').sort_values(by=['REGION_AVG_CONST', 'YEAR'], ascending=[True, True])

# Group the DataFrame object by the column(s) that will be kept as identifier variables,
# then calculate the mean of the values in the column(s) that will be kept as identifier variables,
# then reset the index of the DataFrame object
df_hs_pivot_groupby = df_hs_pivot.groupby(['REGION_AVG_HS', 'YEAR'])['AVG_HS_PRICE'].mean().reset_index()
df_pop_pivot_groupby = df_pop_pivot.groupby(['REGION_POP_EST', 'YEAR'])['POP_AVG'].sum().reset_index()

# Join the DataFrame objects by the column(s) that will be kept as identifier variables,set_index() is used to set the DataFrame object index using existing columns,
# then join the DataFrame objects by the index of the DataFrame objects, 
# then reset the index of the DataFrame object
df_hs_const_join =  df_hs_pivot_groupby.join(df_const_pivot.set_index(['REGION_AVG_CONST', 'YEAR']), on=['REGION_AVG_HS', 'YEAR'], how='left', lsuffix='_hs', rsuffix='_const')
df_hs_const_pop_join = df_hs_const_join.join(df_pop_pivot_groupby.set_index(['REGION_POP_EST', 'YEAR']), on=['REGION_AVG_HS', 'YEAR'], how='left', lsuffix='_hs_const', rsuffix='_pop')

df_house_market = df_hs_const_pop_join[['REGION_AVG_HS', 'YEAR', 'AVG_HS_PRICE', 'NCONST', 'POP_AVG']]

df_house_market['REGION_AVG_HS'] = df_house_market['REGION_AVG_HS'].astype(str)
df_house_market['AVG_HS_PRICE'] = df_house_market['AVG_HS_PRICE'].astype(int)
df_house_market['NCONST'] = df_house_market['NCONST'].astype(int)
df_house_market['POP_AVG'] = df_house_market['POP_AVG'].astype(int)

# Create a list of the column(s) that will be kept as identifier variables
feature_cols1 = ['AVG_HS_PRICE', 'NCONST']
feature_cols2 = ['AVG_HS_PRICE', 'POP_AVG']
feature_cols3 = ['AVG_HS_PRICE', 'NCONST', 'POP_AVG']

# Calculate the correlation between the column(s) that will be kept as identifier variables
feature_cor1 = df_house_market[feature_cols1].corr()
feature_cor2 = df_house_market[feature_cols2].corr()
feature_cor3 = df_house_market[feature_cols3].corr()

nrtheast_filter = df_house_market['REGION_AVG_HS'] == 'NthEast'
midwest_filter = df_house_market['REGION_AVG_HS'] == 'MidWest'
south_filter = df_house_market['REGION_AVG_HS'] == 'South'
west_filter = df_house_market['REGION_AVG_HS'] == 'West'

# nrtheast_corr = df_house_market[nrtheast_filter][feature_cols3].corr()

nrtheast_corr = df_house_market[nrtheast_filter][feature_cols3].corr()
midwest_corr = df_house_market[midwest_filter][feature_cols3].corr()
south_corr = df_house_market[south_filter][feature_cols3].corr()
west_corr = df_house_market[west_filter][feature_cols3].corr()

nrtheast_df = pd.DataFrame(nrtheast_corr)

print(nrtheast_df, midwest_corr, south_corr, west_corr) 
#df_hs_const_pop_join.to_csv(r'C:\housing_market_analysis\HS_CONST_POP_2009_2022.csv', index=False)

# server = 'LAPTOP-2HPO7SKP'
# database = 'HOUSING_MARKET_ANALYSIS'
# username = 'tdolan'
# password = 'BaroN098'

# # Create a connection string
# connection_string = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

# # Create a connection and cursor
# connection = db.connect(connection_string)
# cursor = connection.cursor()

# # Create the engine to connect to the PostgreSQL database
# engine = sa.create_engine('mssql+pyodbc://tdolan:BaroN098@LAPTOP-2HPO7SKP/HOUSING_MARKET_ANALYSIS?driver=ODBC+Driver+17+for+SQL+Server')

# df_house_market.to_sql('HSAVG_NCONST_POPAVG', engine, if_exists='replace', index=False)

# # Close the cursor and connection
# cursor.close()
