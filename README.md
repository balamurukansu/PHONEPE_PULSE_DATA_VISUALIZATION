# Data Visualization and Exploration : A User-Friendly Tool Using Streamlit and Plotly

# What is PhonePe Pulse?
  The [PhonePe Pulse website](https://www.phonepe.com/pulse/explore/transaction/2022/4/) showcases more than 2000+ Crore transactions by consumers on an interactive map of India. With over 45% market share, PhonePe's data is representative of the country's digital payment habits.
The insights on the website and in the report have been drawn from two key sources - the entirety of PhonePe's transaction data combined with merchant and customer interviews. The report is available as a free download on the [PhonePe Pulse website](https://www.phonepe.com/pulse/explore/transaction/2022/4/) and [GitHub](https://github.com/PhonePe/pulse).

# Libraries/Modules needed for the project!

 1. [Plotly](https://plotly.com/python/) - (To plot and visualize the data)
 2. [Pandas](https://pandas.pydata.org/docs/) - (To Create a DataFrame with the scraped data)
 3. mysql.connector - (To store and retrieve the data)
 4. [Streamlit](https://docs.streamlit.io/library/api-reference) - (To Create Graphical user Interface)
 5. json - (To load the json files)
 6. git.repo.base - (To clone the GitHub repository)
 
 # Workflow
 
 ### Step 1:
 
 **Importing the Libraries:**
 
   Importing the libraries. As I have already mentioned above the list of libraries/modules needed for the project. First we have to import all those libraries. If the libraries are not installed already use the below piece of code to install.

        !pip install ["Name of the library"]
    
   If the libraries are already installed then we have to import those into our script by mentioning the below codes.

        import pandas as pd
        import mysql.connector as sql
        import streamlit as st
        import plotly.express as px
        import os
        import json
        from streamlit_option_menu import option_menu
        from PIL import Image
        from git.repo.base import Repo
 
 
 ### Step 2:
 
 **Data extraction:** 

   Clone the Github using scripting to fetch the data from the Phonepe pulse Github repository and store it in a suitable format such as JSON. Use the below syntax to clone the phonepe github repository into your local drive.
    
        from git.repo.base import Repo
        Repo.clone_from("GitHub Clone URL","Path to get the cloded files")
      
 ### Step 3:
 
 **Data transformation:**
 
   In this step the JSON files that are available in the folders are converted into the readeable and understandable DataFrame format by using the for loop and iterating file by file and then finally the DataFrame is created. In order to perform this step I've used **os**, **json** and **pandas** packages.
   
   
    path1 = "Path of the JSON files"
    agg_trans_list = os.listdir(path1)

    #Give a name to columns
    columns1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],'Transaction_amount': []}
    
    
Looping through each and every folder and opening the json files appending only the required key and values and creating the dataframe.


col_au = {'State':[],'Year':[],'Quarter':[],'brand':[],'count':[],'percentage':[]}
for i in Agg_user_list:
    p_i=path2 + i + '/'   
    agg_yr=os.listdir(p_i)   
    for j in agg_yr:
        p_j=p_i + j + '/'   
        agg_yr=os.listdir(p_j)
        for k in agg_yr:
            p_k = p_j + k          
            data = open(p_k,'r')
            D=json.load(data)
            # print(D)
            try:
                for x in D['data']['usersByDevice']:
                    # print(x)
                    brand = x['brand']
                    count = x['count']
                    percentage = x['percentage']
                
                    col_au['State'].append(i)
                    col_au['Year'].append(j)
                    col_au['Quarter'].append(k.strip('.json'))
                    col_au['brand'].append(brand)
                    col_au['count'].append(count)
                    col_au['percentage'].append(percentage)
            except:
                pass


Aggr_user = pd.DataFrame(col_au)   
             
 ### Step 4:
 
 **Database insertion:**
 
   To insert the datadrame into SQL first I've created a new database and tables using **"mysql-connector-python"** library in Python to connect to a MySQL database and insert the transformed data using SQL commands.
   
   **Creating the connection between python and mysql**
   
        mydb = sql.connect(host="localhost",
                   user="username",
                   password="password",                   
                  )
        mycursor = mydb.cursor(buffered=True)
        
   **Creating tables**
   
    create_Aggr_trans='''create table if not exists Aggr_trans (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year   
    int,Quarter int,Transaction_Type varchar(100),Transaction_count int,Transaction_amount double)'''
    cursor.execute(create_Aggr_trans)
    conn.commit()


        for i,row in df.iterrows():        
            #%S refers to string values 
            for i,row in Aggr_trans.iterrows():
            insert_Aggr_trans = "insert ignore into Aggr_trans (State,Year,Quarter,Transaction_Type,Transaction_count,Transaction_amount) values (%s,%s,%s,%s,%s,%s)"
        cursor.execute(insert_Aggr_trans,tuple(row))
        
            
           #data is not auto committed by default, so we must commit to save our changes
           conn.commit()
    
 ### Step 5:
 
 **Dashboard creation:**
 
   To create colourful and insightful dashboard I've used Plotly libraries in Python to create an interactive and visually appealing dashboard. Plotly's built-in Pie, Bar, Geo map functions are used to display the data on a charts and map and Streamlit is used to create a user-friendly interface with multiple dropdown options for users to select different facts and figures to display.
    
 ### Step 6:
 
 **Data retrieval:**
 
   Finally if needed Using the "mysql-connector-python" library to connect to the MySQL database and fetch the data into a Pandas dataframe.
