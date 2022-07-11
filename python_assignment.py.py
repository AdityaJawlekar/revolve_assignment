#!/usr/bin/env python
# coding: utf-8

# # Importing Libraries

# In[ ]:


import os
import json
import pandas as pd
import psycopg2
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


# # Extracting Dataset

# ## 1. Customer Dataset

# In[ ]:


customer_data = pd.read_csv('D:/Study/Data_Science/DS_Assignments/04_Revolve/input_data/starter/customers.csv')


# ## 2. Product Dataset

# In[ ]:


product_data = pd.read_csv('D:/Study/Data_Science/DS_Assignments/04_Revolve/input_data/starter/products.csv')
product_data = product_data.drop('product_description', axis=1)


# ## 3. Transactions Dataset

# ### Reading all json files using for loop and append the data

# In[ ]:


dir_list = os.listdir(r'D:/Study/Data_Science/DS_Assignments/04_Revolve/input_data/starter/transactions/')
res_list = []
path = r'D:/Study/Data_Science/DS_Assignments/04_Revolve/input_data/starter/transactions/'
c = 0
for i in dir_list:
    c = c+1
    if c == 100:
        break
    pfinal = os.path.join(path, i, 'transactions.json')
    data = pd.read_json(pfinal, lines=True, orient='columns')
    res_list.append(data)


# ### Converting the data into required pandas data frame

# In[ ]:


df = pd.concat(res_list, ignore_index=True)
transaction_data = pd.DataFrame(df)
transaction_data = transaction_data.explode('basket').reset_index(drop=True)
transaction_data = transaction_data.merge(pd.json_normalize(transaction_data['basket']), left_index=True, right_index=True).drop(['basket','price','date_of_purchase'],axis=1)


# ## Merge the customer and transaction data on customer_id column

# In[ ]:


join_1 = pd.merge(right = customer_data,
                   left = transaction_data, 
                   right_on = 'customer_id', 
                   left_on = 'customer_id',
                   how = "inner")


# ## Merge the above result and product data on product_id column

# In[ ]:


join_2 = pd.merge(right = join_1,
                   left = product_data, 
                   right_on = 'product_id', 
                   left_on = 'product_id',
                   how = "inner")


# ## Saving the result

# In[ ]:


join_3 = pd.DataFrame(data=join_2)


# ## Rearranging the data frame in required format

# In[ ]:


final_data = join_3
final_data = final_data.sort_values(by=['customer_id']).reset_index()
final_data = final_data.drop('index', axis=1)
column_titles = ['customer_id', 'loyalty_score', 'product_id', 'product_category']
final_data = final_data.reindex(columns = column_titles)


# # Transform the data using postgresql 

# ## Creating connection with postgresql and creating new database

# In[ ]:


def create_database():
    try:
        # connect to default database
        conn = psycopg2.connect('host=127.0.0.1 dbname=postgres user=postgres password=********')
        conn.set_session(autocommit = True)
        cur = conn.cursor()
        
        # creating new database as revolvedb
        cur.execute('DROP DATABASE IF EXISTS revolvedb')
        cur.execute('CREATE DATABASE revolvedb')
    
        #close connection with default database
        conn.close()
        
    except psycopg2.Error as e:
        print('Error: Could not make connection to the postgres database')
        print(e)
        
    try:
        # connect to new database
        conn = psycopg2.connect('host=127.0.0.1 dbname=revolvedb user=postgres password=********')
        cur = conn.cursor()
    
    except psycopg2.Error as e:
        print('Error: Could not make connection to the postgres database')
        print(e)
    
    return cur, conn


# In[ ]:


try:
    cur, conn = create_database()
    
except psycopg2.Error as e:
    print(e)


# ### Creating table 'output'

# In[ ]:


try:
    output_table = ('''CREATE TABLE IF NOT EXISTS output(customer_id VARCHAR,
                     loyalty_score INT, product_id VARCHAR, product_category VARCHAR)''')
    
except psycopg2.Error as e:
    print(e)


# In[ ]:


try:
    cur.execute(output_table)
    
except psycopg2.Error as e:
    print(e)


# ### Inserting values into  table 'output'

# In[ ]:


try:
    output_table_insert = ('''INSERT INTO output(customer_id, loyalty_score, product_id, product_category)
                            VALUES(%s, %s, %s, %s)''')

except psycopg2.Error as e:
    print(e)    


# In[ ]:


for i, row in final_data.iterrows():
    cur.execute(output_table_insert, list(row))


# ### Getting purchase_count as a new column and save the result as a output_data.csv file

# In[ ]:


try:
    purchase_table = ('''COPY(   SELECT customer_id, product_id, product_category, COUNT(product_id) AS purchase_count
                               FROM output
                           GROUP BY customer_id, product_category, product_id
                           ORDER BY customer_id, product_id)
                         TO 'D:\output_data.CSV' DELIMITER ',' CSV HEADER''')

except psycopg2.Error as e:
    print(e)


# In[ ]:


try:
    cur.execute(purchase_table)
    
except psycopg2.Error as e:
    print(e)


# In[ ]:


try:
    conn.commit()

except psycopg2.Error as e:
    print(e)


# In[ ]:


try:
    conn.close()

except psycopg2.Error as e:
    print(e)


# ### Read the output_data.csv file

# In[ ]:


purchase_table = pd.read_csv('D:/output_data.CSV')


# ### Load the data as result_data.json file

# In[ ]:


result_data = purchase_table.to_json('D:/result_data.json', orient ='split', compression = 'infer')


# In[ ]:




