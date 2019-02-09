"""
	A set of functions that make spark sql slightly easier 
	(in it's current primitive state)
"""

import pandas as pd
import numpy as np


# Define JDBC Connections
sqlserver = "xxx"
port = "xxx"
database = "xxx"
user = "xxx"
pswd = "xxx"
jdbcUrl = "jdbc:sqlserver://"+sqlserver+":"+port+";database="+database
print(jdbcUrl)



tempTableList= []
def registerTable(tableName,tempTableName, printSchema="N"):
  if(tempTableName in tempTableList):
    print("Table with Temp table name already Registered")
    return tempTableList[tempTableName]
  else:
    tableDataFrame = spark.read.option('user', user).option('password', pswd).jdbc(jdbcUrl,tableName)
    tableDataFrame.registerTempTable(tempTableName)
    tempTableList.append(tempTableName)
    return tableDataFrame
  
def dropTempTable(tempTableName):
  dropTableQuery = "DROP TABLE IF EXISTS "+tempTableName
  spark.sql(dropTableQuery)
  #if(len(tempTableList)>0):
  if(tempTableName in tempTableList):
    tempTableList.remove(tempTableName)
    
def dropAllTempTables():
  for tableName in tempTableList:
    dropTableQuery = "DROP TABLE IF EXISTS "+tableName
    spark.sql(dropTableQuery)
  tempTableList.clear()
  
def registeredTableList():
  return tempTableList

def VIEW_TABLE(tb, n=50):
  pushdown_qry="(SELECT TOP {num_record} * FROM {table}) temp_alias".format(table=tb, num_record=n)
  sdf_viewtable = spark.read.option('user', user).option('password', pswd).jdbc(url=jdbcUrl,table=pushdown_qry)
  sdf_viewtable.createOrReplaceTempView('VIEWTABLE')
  display(sdf_viewtable)
  
def QUERY_TABLE(qry, out='QUERY_OUTPUT', register=True):
  sdf_qry_output = spark.read.option('user', user).option('password', pswd).jdbc(url=jdbcUrl,table="( {qry} ) temp_alias".format(qry=qry))
  
  if register:
    sdf_qry_output.createOrReplaceTempView(out)
    stdout = "You can now access {out} with %sql command, {out} contains {n_row} rows".format(out=out, n_row=sdf_qry_output.count())
  else:
    stdout = "The results of the query is is a sparkdataframe with {n_row} rows".format(n_row=sdf_qry_output.count())
    
  print("Query executed sccessfully: {stdout}".format(out=out, stdout=stdout))
  return sdf_qry_output



def QUERY_TABLE_CACHE(qry, 
                      temp_name='QUERY_OUTPUT', 
                      register=True, 
                      cache=True, 
                      persist_db = None,
                      persist_name = 'dbs_persist_temp',
                      replace_persist=False):
  """
     ----------------------------------
     RUN QUERY FROM JDBC INTO SPARK
     ----------------------------------
     Step 1) Run string query in jdbc connection into spark
     Step 2) Register as tempory Table
     Step 3) Cache table into memory for speed of query
     Step 4) Copy this table to persisitant table in stored hive database 
     Step 5) Returns a pyspark dataframe (You can assign to an object)
     
     Params:
       register   : [True,False]
       cache      : [True,False]
       persist_db : Set to None if do not want to persist table
       replace_persist : if you want to replace the existing persistant table
     
  """
  
  #Step 1
  sdf_qry_output = spark.read.option('user', user).option('password', pswd).jdbc(url=jdbcUrl,table="( {qry} ) temp_alias".format(qry=qry))
  
  #Step 2
  if register:
    sdf_qry_output.createOrReplaceTempView(temp_name)
    stdout = "You can now access {out} with %sql command, {out} contains {n_row} rows".format(out=temp_name, n_row=sdf_qry_output.count())
  else:
    stdout = "The results of the query is is a sparkdataframe with {n_row} rows".format(n_row=sdf_qry_output.count())
  print("Step 1,2: Query executed sccessfully: {stdout}".format(out=temp_name, stdout=stdout))
  
  #Step 3
  if cache==True:
    spark.sql("cache table {0}".format(temp_name))
    print("Step 3: Cached table {0}".format(temp_name))
  else:
    print('Step 3: Not cached')
    
  #Step 4
  if persist_db:
    spark.sql("use {0}".format(persist_db))
    # check if already persists
    tbls = spark.sql("show tables")
    tbls_lst = tbls[tbls.database==persist_db].select('tableName').rdd.flatMap(lambda x: x).collect()
    
    if replace_persist == True:
      spark.sql("drop table if exists {0}".format(persist_name))
      spark.sql("use {0}".format(persist_db))
      spark.sql("create table if not exists {0} as select * from {1}".format(persist_name,temp_name))
      print('Step 4: Replaced existing persisted Table in {0} as {1} '.format(persist_db,persist_name))
    
     
    elif persist_name in tbls_lst: 
      print('Step 4: Table already persisted in {0} as {1}. Please drop to use:'.format(persist_db,persist_name))
      drp_str1 = 'spark.sql("use {0}")'.format(persist_db)
      drp_str2 = 'spark.sql("drop table {0}")'.format(persist_name)
      print('*'*50)
      print(drp_str1+'\n'+drp_str2) 
      print('*'*50)
      
    else:
      spark.sql("use {0}".format(persist_db))
      spark.sql("create table if not exists {0} as select * from {1}".format(persist_name,temp_name))
      print('Step 4: Table now persisted in {0} as {1} '.format(persist_db,persist_name))

  else:
    print('Step 4: Chosen not to persist Table') 
    
  #Step 5
  print('Complete' )
  return sdf_qry_output

# ###############
# # EXAMPLE
# ###############
# QUERY = """ SELECT * FROM table_name """

# # run, register, cache & persist
# pyspark_tbl = QUERY_TABLE_CACHE(QUERY, 
#                     temp_name='xxx',
#                     persist_name = 'xxx',
#                     register=True, 
#                     cache=True, 
#                     persist_db = 'dbs_db')



def distinct_cols_multi_table(table_list=[], remove_columns=['PNR_LOC','PNR_CRT_DTE'], output_format='sql'):
  """ 
      --------------------------------------------------------
      FIND SET OF UNIQUE COLUMN NAMES FROM MULTIPLE TABLE
      --------------------------------------------------------
      TSQL does not automatically combine columns with same name from different tables
      This creates a distinct list of columns with table references A-Z 
      
      remove_columns: You man also want to remove columns 
      i.e inconsistant naming from two table PNR_LOC/PNR_LOCATOR
      
      output_format = 'sql' reutrn syntax appropriate for %SQL 
      output_format = 'pyspark' reutrn syntax appropriate for pyspark
      
  """
  alfa = "abcdefghijklmnopqrstuvwxyz"
  
  all_cols=set()
  all_cols_str=''
  
  for i, table in enumerate(table_list):
    cols=table.columns

    #find new cols (cols in B but not in A)
    different_cols = set(table.columns)-all_cols

    #remove additional removal cols
    different_cols = different_cols-set(remove_columns)

    #append table references
    if output_format == 'sql':
      ref_cols = ',{0}.'.format(alfa[i])+', {0}.'.join(different_cols).format(alfa[i])
      #append to full column list & str
      all_cols = all_cols.union(different_cols)
      all_cols_str = all_cols_str +""+ ref_cols
    
    elif output_format=='pyspark':
      ref_cols = ','.join('{1}.{0}'.format(w,alfa[i]) for w in different_cols)
      #append to full column list & str
      all_cols = all_cols.union(different_cols)
      all_cols_str = all_cols_str +","+ ref_cols
    else:
      print("please select output_format = ['pyspark','sql']")
    

  # remove first comma
  if output_format == 'sql':
    return all_cols_str[1:]
  elif output_format=='pyspark':
    return all_cols_str[1:].split(',')
