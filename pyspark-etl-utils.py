
"""
	A set of functions that make ETA in pyspark slightly easier 
	(in it's current primitive state)

	Focus on debugging data flows whilst building
"""

import collections
from pyspark.sql.functions import col, regexp_replace


def distinct_cols_multi_table_pysprk(table_list=[], remove_columns=['col_names'], output_format='sql'):
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
  
  
    
def outerjoin_pydf(TableA,TableB,keys, verbose=0):
  """ 
      --------------------------------------------------------
      PERFORM OUTER JOIN AND REMOVE DUPE NON JOINING KEYS
      --------------------------------------------------------
      
      Verbose >1: left/right debug counts
      
  """

  #remove dupe cols from 2 dfs
  unique_cols = distinct_cols_multi_table_pysprk(table_list=[TableA,TableB], output_format='pyspark')

  # now remove alias for joined columns
  unique_cols = [x[2:] if x[2:] in keys else x for x in unique_cols]
  
  # add count key (Counting nulls across multiple col = a nightmare in spark)
  TableA = TableA.withColumn('TableA', lit(1))
  TableB = TableB.withColumn('TableB', lit(1))
  unique_cols = unique_cols+['TableA','TableB']
  
  # do outer join
  outerTable = TableA.alias("a").join(TableB.alias("b"), keys, how='outer').select(unique_cols)
  
  
  # counts
  if verbose>0:
    total_predeupe = outerTable.count()
  
  # drop dupes
  outerTable = outerTable.drop_duplicates()
  
  # more counts
  if verbose>0:
    l_nonnull = outerTable.select(F.sum('TableA')).collect()[0][0]
    r_nonnull = outerTable.select(F.sum('TableB')).collect()[0][0]
    total = outerTable.count()

    print("-"*38)
    print("OUTER JOIN UNQIUE COLS: DEBUG COUNTS")
    print("-"*38)
    
    print('Outer Original :',total_predeupe)
    print('Outer Deduped  :',total)

    print('\nLeft  NonNull  :',l_nonnull)
    print('Left  Null     :',total-l_nonnull)
    print('Right NonNull  :',r_nonnull)
    print('Right Null     :',total-r_nonnull)
    print("-"*38,'\n')

    #remove unnecersary
    outerTable = outerTable.drop('TableA')
    outerTable = outerTable.drop('TableB')
  
  return outerTable


def check_keys_dtypes(keys, df1, df2):
  print("-"*60,"\nCheck Key dtypes\n"+"-"*60)
  for key in keys:
    df1_key_dtype = [x[1] for x in df1.dtypes if x[0] == key][0]
    df2_key_dtype = [x[1] for x in df2.dtypes if x[0] == key][0]
    print(df1_key_dtype == df2_key_dtype, "("+key+")", df1_key_dtype, df2_key_dtype, )



    
def change_col_dtype(table, col, type, verbose=0):
  """ 
      REASON: Make dataflows fault tolerant
              If you try and change a col to a format it already is, spark throws an error
  """
  if dict(table.dtypes)[col]!=type:
    table = table.withColumn(col, table[col].cast(type))
    if verbose>0:
      print(col,'changed to dtype:',type)
  else:
    print(col,'Not changed, already dtype:',type)
  return table


def get_dup_cols(df):
  print("-"*60,"\nShowing Duplicate Columns\n"+"-"*60)
  col_count = collections.Counter(df.columns)
  for k in col_count:
    if col_count[k]>1:
      print(k)
  if max(col_count.values()) ==1: print("{}")


def get_col_mapping(df):
  for x, y in df.dtypes:
    print("'"+x+"'"+" "*(30-len(x))+":'"+x+"',")
    

def remove_spaces(df, columns):
  for column in columns:
    df = df.withColumn(column, regexp_replace(col(column), ' ', ''))
  return df
  
  
def remove_columns(df, columns):
  for column in columns:
    df = df.drop(column)
  return df
  
  
def remap(df, col_mapping):
  for k, v in col_mapping.items():
    df = df.withColumnRenamed(k, v)
  return df.select(list(col_mapping.values()))


def check_renamed_cols(cols_dict, table_name='table_name'):
  print("-"*60,"\nRenamed Columns in "+str(table_name)+"\n"+"-"*60)
  diff = cols_dict.keys()-cols_dict.values()
  if diff:
    for d in diff:
      print("'"+d+"'"+" "*(30-len(d))+":'"+cols_dict[d]+"',")
  else:
    print("{}")
  
def show_all_pydfs(locals):
  dict_locals = dict(locals)
  for d in dict_locals:
    if str(type(dict_locals[d]))=="<class 'pyspark.sql.dataframe.DataFrame'>":
      #Stored dataframes such as _103
      if d[0]!='_':
        print(d)

        
def get_shape(df, name=None):
  if name:
    print("{} shape: ({}, {})".format(name, df.count(), len(df.columns)))
  else:
    print("Dataframe shape: ({}, {})".format(df.count(), len(df.columns)))
   
  
 


        