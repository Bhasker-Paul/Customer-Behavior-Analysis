#!/usr/bin/env python
# coding: utf-8

# # Import Library

# In[110]:


import numpy as np
import pandas as pd


# In[111]:


df=pd.read_csv("E:\Data Set\customer_shopping_behavior.csv")


# In[112]:


df


# In[113]:


df.info()


# In[114]:


df.isnull().sum()


# In[115]:


df['Review Rating'].fillna(df['Review Rating'].median(),inplace=True)
df['Review Rating'].astype(float)


# In[116]:


df.isnull().sum()


# In[117]:


df.columns=df.columns.str.lower()


# In[118]:


df.columns


# In[119]:


df.columns=df.columns.str.replace(" ","_")


# In[120]:


df.columns


# In[139]:


df.rename({'purchase_amount_(usd)':'purchase_amount'},axis=1,inplace=True)


# In[122]:


labels=['young_adult','adult','middle_aged','senior_citizen']
df['age_group']=pd.qcut(df['age'],q=4,labels=labels)


# In[123]:


df[['age','age_group']].head()


# In[124]:


frequency_mapping={'Fortnightly':14,
                 'Weekly':7,'Monthly':30,
                'Bi-Weekly':15,'Annually':365,'Quarterly':90, 'Every 3 Months':90 }


# In[125]:


df['frequency_of_purchase_days']=df['frequency_of_purchases'].map(frequency_mapping)


# In[126]:


df['frequency_of_purchase_days']=df['frequency_of_purchase_days'].fillna(df['frequency_of_purchase_days'].median()).astype(int)


# In[127]:


df[['frequency_of_purchases','frequency_of_purchase_days']].head()


# In[128]:


df[['discount_applied','promo_code_used']].head()


# In[129]:


(df['discount_applied']==df['promo_code_used']).all()


# In[130]:


df.drop('promo_code_used',axis=1,inplace=True)


# In[22]:


get_ipython().system('pip install sqlalchemy')


# In[23]:


get_ipython().system('pip install pyodbc sqlalchemy')


# In[131]:


import mysql.connector as connection

try:
  
    mydb = connection.connect(
        host='localhost',
        user='Bhasker',
        password='abcd1234@',
        use_pure=True
    )

    if mydb.is_connected():
        cursor = mydb.cursor()
        # database create
        cursor.execute("CREATE DATABASE IF NOT EXISTS customer_behavior")
        print("Database customer_behavior exists or created!")

        # database use
        cursor.execute("USE customer_behavior")
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()
        print("Currently using database:", current_db[0])

except Exception as e:
    print("Error:", e)

finally:
    if 'mydb' in locals() and mydb.is_connected():
        mydb.close()


# In[25]:


get_ipython().system('pip install pymysql sqlalchemy')


# In[105]:


from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy import create_engine


# In[132]:


from sqlalchemy import create_engine
import pandas as pd

username = "Bhasker"
password = "abcd1234@"   # keep the @ here if it's part of your password
host = "127.0.0.1"
port = "3306"
database = "customer_behavior"

engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
)


# In[133]:


# Bulk insert with chunksize for large data
df.to_sql("shopping_behavior", engine, if_exists="replace", index=False, chunksize=5000)

print("Data successfully transferred from Notebook to MySQL!")


# In[141]:


username = "Bhasker"
password = "abcd1234@"
host = "localhost"
port = "3306"
database = "customer_behavior"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

# Write DataFrame to MySQL
table_name = "customer"   # choose any table name
df.to_sql(table_name, engine, if_exists="replace", index=False)

# Read back sample
pd.read_sql("SELECT * FROM customer LIMIT 5;", engine)


# In[ ]:




