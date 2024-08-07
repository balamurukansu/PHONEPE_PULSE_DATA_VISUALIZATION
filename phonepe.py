# Importing Libraries
import git
import os
import json
import pandas as pd
import PIL 
from PIL import Image
import mysql.connector
from mysql.connector import Error
import streamlit as st
from streamlit_option_menu import option_menu
from git.repo.base import Repo
from streamlit_extras.add_vertical_space import add_vertical_space
import plotly.express as px

# Data Extraction
# To clone the Github Pulse repository use the following code \
# Reference Syntax - Repo.clone_from("Clone Url", "Your working directory") \
# repository_url ="https://github.com/PhonePe/pulse.git"
# destination_directory = "D:/PHONEPAY/data"
# git.Repo.clone_from(repository_url, destination_directory)

#-------------------------------------------------- MySQL connection details
conn = mysql.connector.connect(
    host='199.79.63.133',
    user='sgptrrzs_phonepe',    
    password='G3EJxwK42zmYFuzrBQrhBQz' 
)

#Create cursor
cursor = conn.cursor()

# Create a new database if it does not exist
cursor.execute("CREATE DATABASE IF NOT EXISTS sgptrrzs_phonepe")

# Switch to 'phonepe pulse' database
cursor.execute("USE sgptrrzs_phonepe")

#--------------------------------------------------Insert into MySQL

upload_to_mysql = 'NO'

if upload_to_mysql == 'YES':  
    #--------------------------------------------------Data transformations

    #--------------------------------------------------Dataframe of aggregated Transactions
    path1="D:/DataScience/Phonepe_Pulse_Data_Visualization_and_Exploration/pulse/data/aggregated/transaction/country/india/state/"
    Agg_trans_list=os.listdir(path1)
    # Agg_trans_list
    col_at = {'State':[],'Year':[],'Quarter':[],'Transaction_Type':[],'Transaction_count':[],'Transaction_amount':[]}
    #Extractiing aggregated transactions
    for i in Agg_trans_list:
        p_i=path1 + i + '/'   
        agg_yr=os.listdir(p_i)   
        
        for j in agg_yr:
            p_j=p_i + j + '/'
            agg_yr=os.listdir(p_j)
            
            for k in agg_yr:
                p_k = p_j + k          
                data = open(p_k,'r')
                D=json.load(data)
                
                for x in D['data']['transactionData']:
                    # print(x)
                    Transaction_Type = x['name']
                    Transaction_count = x['paymentInstruments'][0]['count']
                    Transaction_amount = x['paymentInstruments'][0]['amount']
                    
                    col_at['State'].append(i)
                    col_at['Year'].append(j)
                    col_at['Quarter'].append(int(k.strip('.json')))
                    col_at['Transaction_Type'].append(Transaction_Type)
                    col_at['Transaction_count'].append(Transaction_count)
                    col_at['Transaction_amount'].append(Transaction_amount)
                    # print(i,j,k,Transaction_Type,Transaction_count,Transaction_amount)

    Aggr_trans = pd.DataFrame(col_at)
    # Aggr_trans
                
    #--------------------------------------------------Dataframe of aggregated users
    path2="D:/DataScience/Phonepe_Pulse_Data_Visualization_and_Exploration/pulse/data/aggregated/user/country/india/state/"
    Agg_user_list=os.listdir(path2)
    # Agg_user_list

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
    # Aggr_user      

    #--------------------------------------------------Dataframe of mapped transaction
    path3="D:/DataScience/Phonepe_Pulse_Data_Visualization_and_Exploration/pulse/data/map/transaction/hover/country/india/state/"
    map_trans_list=os.listdir(path3)
    # map_trans_list

    col_mt = {'State':[],'Year':[],'Quarter':[],'Transaction_Type':[],'Transaction_count':[],'Transaction_amount':[]}
    for i in map_trans_list:
        p_i=path3 + i + '/'   
        agg_yr=os.listdir(p_i)   
        
        for j in agg_yr:
            p_j=p_i + j + '/'
            agg_yr=os.listdir(p_j)
            
            for k in agg_yr:
                p_k = p_j + k          
                data = open(p_k,'r')
                D=json.load(data)
                
                for x in D['data']['hoverDataList']:
                    # print(x)
                    Transaction_Type = x['metric'][0]['type']
                    Transaction_count = x['metric'][0]['count']
                    Transaction_amount = x['metric'][0]['amount']
                    
                    col_mt['State'].append(i)
                    col_mt['Year'].append(j)
                    col_mt['Quarter'].append(k.strip('.json'))
                    col_mt['Transaction_Type'].append(Transaction_Type)
                    col_mt['Transaction_count'].append(Transaction_count)
                    col_mt['Transaction_amount'].append(Transaction_amount)

    Map_trans = pd.DataFrame(col_mt)
    # Map_trans   

    #--------------------------------------------------Dataframe of mapped user
    path4="D:/DataScience/Phonepe_Pulse_Data_Visualization_and_Exploration/pulse/data/map/user/hover/country/india/state/"
    map_user_list=os.listdir(path4)
    # map_user_list

    col_mu = {'State':[],'Year':[],'Quarter':[],'district':[],'registeredUsers':[],'appOpens':[]}
    for i in map_user_list:    
        p_i=path4 + i + '/'   
        agg_yr=os.listdir(p_i)   
        
        for j in agg_yr:        
            p_j=p_i + j + '/'
            agg_yr=os.listdir(p_j)
            
            for k in agg_yr:
                p_k = p_j + k          
                data = open(p_k,'r')
                D=json.load(data)
                for x in D['data']['hoverData'].items():                                
                    district = x[0]
                    registeredUsers = x[1]['registeredUsers']
                    appOpens = x[1]['appOpens']             
                    
                    col_mu['State'].append(i)
                    col_mu['Year'].append(j)
                    col_mu['Quarter'].append(k.strip('.json'))
                    col_mu['district'].append(district)
                    col_mu['registeredUsers'].append(registeredUsers)
                    col_mu['appOpens'].append(appOpens)

    Map_users = pd.DataFrame(col_mu)
    # Map_users

    #--------------------------------------------------#Dataframe of top transactions
    path5="D:/DataScience/Phonepe_Pulse_Data_Visualization_and_Exploration/pulse/data/top/transaction/country/india/state/"
    top_trans_list=os.listdir(path5)
    # top_trans_list

    col_tt = {'State':[],'Year':[],'Quarter':[],'pincode':[],'Transaction_Type':[],'Transaction_count':[],'Transaction_amount':[]}
    for i in top_trans_list:    
        p_i=path5 + i + '/'   
        agg_yr=os.listdir(p_i)   
        
        for j in agg_yr:        
            p_j=p_i + j + '/'
            agg_yr=os.listdir(p_j)
            
            for k in agg_yr:
                p_k = p_j + k          
                data = open(p_k,'r')
                D=json.load(data)
                for x in D['data']['pincodes']: 
                    # print(x['metric']['type'])                               
                    pincode = x['entityName']
                    Transaction_Type = x['metric']['type']
                    Transaction_count = x['metric']['count']
                    Transaction_amount = x['metric']['amount']
                    
                    col_tt['State'].append(i)
                    col_tt['Year'].append(j)
                    col_tt['Quarter'].append(k.strip('.json'))
                    col_tt['pincode'].append(pincode)
                    col_tt['Transaction_Type'].append(Transaction_Type)
                    col_tt['Transaction_count'].append(Transaction_count)
                    col_tt['Transaction_amount'].append(Transaction_amount)

    top_trans = pd.DataFrame(col_tt)
    # top_trans

    #--------------------------------------------------Dataframe of top users
    path6="D:/DataScience/Phonepe_Pulse_Data_Visualization_and_Exploration/pulse/data/top/user/country/india/state/"
    top_users_list=os.listdir(path6)
    # top_users_list

    col_tu = {'State':[],'Year':[],'Quarter':[],'pincode':[],'registeredUsers':[]}

    for i in top_users_list:    
        p_i=path6 + i + '/'   
        agg_yr=os.listdir(p_i)   
        
        for j in agg_yr:        
            p_j=p_i + j + '/'
            agg_yr=os.listdir(p_j)
            
            for k in agg_yr:
                p_k = p_j + k          
                data = open(p_k,'r')
                D=json.load(data)
                for x in D['data']['pincodes']: 
                    pincode = x['name']
                    registeredUsers = x['registeredUsers']
                    col_tu['State'].append(i)
                    col_tu['Year'].append(j)
                    col_tu['Quarter'].append(k.strip('.json'))
                    col_tu['pincode'].append(pincode)
                    col_tu['registeredUsers'].append(registeredUsers)
                    
    top_users = pd.DataFrame(col_tu)
    # top_users

#-------------------------------------------------- Uploading to MySQL table  
    # Creating Aggr_trans table
    create_Aggr_trans='''create table if not exists Aggr_trans (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year int,Quarter int,Transaction_Type varchar(100),Transaction_count int,Transaction_amount double)'''
    cursor.execute(create_Aggr_trans)
    conn.commit()

    # Creating agg_user table
    create_Aggr_user='''create table if not exists Aggr_user (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year int,Quarter int,brand varchar(100),count int,percentage double)'''
    cursor.execute(create_Aggr_user)
    conn.commit()

    # Creating map_trans table
    create_Map_trans='''create table if not exists Map_trans (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year int,Quarter int,Transaction_Type varchar(100),Transaction_count int,Transaction_amount double)'''
    cursor.execute(create_Map_trans)
    conn.commit()

    # Creating map_user table
    create_Map_user='''create table if not exists Map_user (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year int,Quarter int,district varchar(100),registeredUsers int,appOpens int)'''
    cursor.execute(create_Map_user)
    conn.commit()

    # Creating top_trans table
    create_top_trans='''create table if not exists top_trans (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year int,Quarter int,pincode int,Transaction_Type varchar(100),Transaction_count int,Transaction_amount double)'''
    cursor.execute(create_top_trans)
    conn.commit()

    # Creating top_user table
    create_top_users='''create table if not exists top_users (slno bigint NOT NULL AUTO_INCREMENT primary key,State varchar(100),Year int,Quarter int,pincode int,registeredUsers int)'''
    cursor.execute(create_top_users)
    conn.commit()

    #-------------------------------------------------- Inserting into Tables
    for i,row in Aggr_trans.iterrows():
        insert_Aggr_trans = "INSERT IGNORE into Aggr_trans (State,Year,Quarter,Transaction_Type,Transaction_count,Transaction_amount) values (%s,%s,%s,%s,%s,%s)"
        # print(insert_Aggr_trans)
        cursor.execute(insert_Aggr_trans,tuple(row))
        conn.commit()

    for i,row in Aggr_user.iterrows():
        insert_Aggr_user = "INSERT IGNORE into Aggr_user (State,Year,Quarter,brand,count,percentage) values (%s,%s,%s,%s,%s,%s)"
        # print(insert_Aggr_user)
        cursor.execute(insert_Aggr_user,tuple(row))
        conn.commit()

    for i,row in Map_trans.iterrows():
        insert_Map_trans = "INSERT IGNORE into Map_trans (State,Year,Quarter,Transaction_Type,Transaction_count,Transaction_amount) values (%s,%s,%s,%s,%s,%s)"
        # print(insert_Map_trans)
        cursor.execute(insert_Map_trans,tuple(row))
        conn.commit()

    for i,row in Map_users.iterrows():
        insert_Map_users = "INSERT IGNORE into Map_user (State,Year,Quarter,district,registeredUsers,appOpens) values (%s,%s,%s,%s,%s,%s)"
        # print(insert_Map_user)
        cursor.execute(insert_Map_users,tuple(row))
        conn.commit()

    for i,row in top_trans.iterrows():
        insert_top_trans = "INSERT IGNORE into top_trans (State,Year,Quarter,pincode,Transaction_Type,Transaction_count,Transaction_amount) values (%s,%s,%s,%s,%s,%s,%s)"
        # print(insert_top_trans)
        cursor.execute(insert_top_trans,tuple(row))
        conn.commit()

    for i,row in top_users.iterrows():
        insert_Aggr_top_users = "INSERT IGNORE into top_users (State,Year,Quarter,pincode,registeredUsers) values (%s,%s,%s,%s,%s)"
        # print(insert_Aggr_top_users)
        cursor.execute(insert_Aggr_top_users,tuple(row))
        conn.commit()
    cursor.close()
    conn.close()

# Create sidebar menu
with st.sidebar:
    st.image(Image.open("images\logo.png"))
    st.markdown('Phonepe Pulse Data Visualization And Exploration: A User-Friendly Tool Using Streamlit And Plotly')
    selected_tab = option_menu(
            menu_title=None,
            options=["Home", "Insights", "Explore Data", "&nbsp;"],
            icons=["house", "bar-chart", "search"], 
            orientation="vertical",
            default_index=0
        )

if selected_tab == "Home":
    l = Image.open("images\logo.png")
    logo = l.resize((250, 100))
    st.image(logo)
    st.subheader("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PhonePe Group is India\’s leading fintech company. Its flagship product, the PhonePe digital payments app, was launched in Aug 2016. Within a short period of time, the company has scaled rapidly to become India’s leading consumer payments app. On the back of its leadership in digital payments, PhonePe Group has expanded into financial services - Insurance, Lending, & Wealth as well as new consumer tech businesses - Pincode and Indus Appstore.")
    st.link_button("Take me to PhonePe", 'https://www.phonepe.com/')

#----------------EXPLORE DATA----------------------#
if selected_tab == "Insights":
    col1,col2 = st.columns([1,1],gap="medium")
    with col1:
        Type = st.radio("**1. Select Type**", ("Transactions", "Users"),horizontal=True)
    with col2:
        Year = st.slider("**2. Choose Year**", min_value=2018, max_value=2024)
        Quarter = st.slider("**3. Choose Quarter**", min_value=1, max_value=4)
    
    st.markdown(f"<h3 style='text-align: center; color: #3E2A80; text-transform:uppercase'>Comprehensive State Data</h3>", unsafe_allow_html=True)
    col1,col2 = st.columns(2)
    
    #ANALYSE DATA
    if Type == "Transactions":
        
        # Comprehensive State Data - TRANSACTIONS AMOUNT - INDIA MAP
        with col1:
            st.markdown(f"<h4 style='color: #3E2A80;'>Amount</h4>", unsafe_allow_html=True)
            # Fetching transaction data
            cursor.execute(f"select state, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from Map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
            df1 = pd.DataFrame(cursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
            cursor.execute(f"SELECT Name FROM statenames")
            df2 = pd.DataFrame(cursor.fetchall(), columns=['Name'])
    
            # Fetching state names
            df1['State'] = df2['Name']
            # st.table(df2)

            fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                      featureidkey='properties.ST_NM',
                      locations='State',
                      color='Total_amount',
                      color_continuous_scale='sunset')

            fig.update_geos(fitbounds="locations", visible=True)
            st.plotly_chart(fig,use_container_width=True)
            
        # Comprehensive State Data - TRANSACTIONS COUNT - INDIA MAP
        with col2:
            st.markdown(f"<h4 style='color: #3E2A80;'>Count</h4>", unsafe_allow_html=True)
            cursor.execute(f"select state, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from Map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
            df1 = pd.DataFrame(cursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
            df1.Total_Transactions = df1.Total_Transactions.astype(int)
            cursor.execute(f"SELECT Name FROM statenames")
            df2 = pd.DataFrame(cursor.fetchall(), columns=['Name'])
    
            # Fetching state names
            df1['State'] = df2['Name']

            fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                      featureidkey='properties.ST_NM',
                      locations='State',
                      color='Total_Transactions',
                      color_continuous_scale='sunset')

            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig,use_container_width=True)
            
            
            
        # BAR CHART OF TOP PAYMENT TYPE
        st.markdown("<h3 color: #3E2A80; text-transform:uppercase'>Most Common Payment Method</h3>", unsafe_allow_html=True)
        cursor.execute(f"select Transaction_type, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from Aggr_trans where year= {Year} and quarter = {Quarter} group by transaction_type order by Transaction_type")
        df = pd.DataFrame(cursor.fetchall(), columns=['Transaction_type', 'Total_Transactions','Total_amount'])

        fig = px.bar(df,
                     title='Transaction Types vs Total_Transactions',
                     x="Transaction_type",
                     y="Total_Transactions",
                     orientation='v',
                     color='Total_amount',
                     color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=False)
        
# BAR CHART TRANSACTIONS - DISTRICT WISE DATA            
        st.markdown("# ")
        st.markdown("# ")
        st.markdown("# ")
        st.markdown(f"<h4 style='color: #3E2A80;'>Select a State to discover more information</h4>", unsafe_allow_html=True)

        selected_state = st.selectbox("",
                             ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                              'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                              'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                              'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                              'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                              'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),index=30)
         
        cursor.execute(f"SELECT State, year, quarter, District, sum(registeredUsers) AS Total_Users, sum(AppOpens) AS Total_Appopens FROM Map_user WHERE year = {Year} AND quarter = {Quarter} AND state = '{selected_state}' GROUP BY State, District, year, quarter ORDER BY state, district")
        
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'year', 'quarter', 'District', 'Total_Users', 'Total_Appopens'])
        df.Total_Users = df.Total_Users.astype(int)
        
        fig = px.bar(df,
                    title=selected_state,
                    x="District",
                    y="Total_Users",
                    orientation='v',
                    color='Total_Users',
                    color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig, use_container_width=True)
        
    # EXPLORE DATA OF USERS      
    if Type == "Users":
        # Comprehensive State Data - TOTAL APPOPENS - INDIA MAP
        cursor.execute(f"select state, sum(registeredUsers) as Total_Users, sum(AppOpens) as Total_Appopens from Map_user where year = {Year} and quarter = {Quarter} group by state order by state")
        df1 = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
        df1.Total_Appopens = df1.Total_Appopens.astype(float)
        cursor.execute(f"SELECT Name FROM statenames")
        df2 = pd.DataFrame(cursor.fetchall(), columns=['Name'])

        # Fetching state names
        df1.State = df2
        
        # BAR CHART TOTAL UERS - DISTRICT WISE DATA 
        st.markdown(f"<h4 style='color: #3E2A80;'>Select a State to discover more information</h4>", unsafe_allow_html=True)
        state_mapping = {'Andaman & Nicobar Islands': 'Andaman-&-Nicobar-Islands',
            'Andhra Pradesh': 'Andhra-Pradesh',
            'Arunachal Pradesh': 'Arunachal-Pradesh',
            'Assam': 'Assam',
            'Bihar': 'Bihar',
            'Chandigarh': 'Chandigarh',
            'Chhattisgarh': 'Chhattisgarh',
            'Dadra & Nagar Haveli & Daman & Diu': 'Dadra-&-Nagar-Haveli-&-Daman-&-Diu',
            'Delhi': 'Delhi',
            'Goa': 'Goa',
            'Gujarat': 'Gujarat',
            'Haryana': 'Haryana',
            'Himachal Pradesh': 'Himachal-Pradesh',
            'Jammu & Kashmir': 'Jammu-&-Kashmir',
            'Jharkhand': 'Jharkhand',
            'Karnataka': 'Karnataka',
            'Kerala': 'Kerala',
            'Ladakh': 'Ladakh',
            'Lakshadweep': 'Lakshadweep',
            'Madhya Pradesh': 'Madhya-Pradesh',
            'Maharashtra': 'Maharashtra',
            'Manipur': 'Manipur',
            'Meghalaya': 'Meghalaya',
            'Mizoram': 'Mizoram',
            'Nagaland': 'Nagaland',
            'Odisha': 'Odisha',
            'Puducherry': 'Puducherry',
            'Punjab': 'Punjab',
            'Rajasthan': 'Rajasthan',
            'Sikkim': 'Sikkim',
            'Tamil Nadu': 'Tamil-Nadu',
            'Telangana': 'Telangana',
            'Tripura': 'Tripura',
            'Uttar Pradesh': 'Uttar-Pradesh',
            'Uttarakhand': 'Uttarakhand',
            'West Bengal': 'West-Bengal'
        }

        # List of display names for the selectbox
        display_names = list(state_mapping.keys())
        selected_state = st.selectbox("",(display_names))
        
        selected_table_value = state_mapping[selected_state]
        cursor.execute(f"select State,year,quarter,District,sum(registeredUsers) as Total_Users, sum(AppOpens) as Total_Appopens from Map_user where year = {Year} and quarter = {Quarter} and state = '{selected_table_value}' group by State, District,year,quarter order by state,district")
        
        df = pd.DataFrame(cursor.fetchall(), columns=['State','year', 'quarter', 'District', 'Total_Users','Total_Appopens'])
        df.Total_Users = df.Total_Users.astype(int)
        
        fig = px.bar(df,
                     title=selected_state,
                     x="District",
                     y="Total_Users",
                     orientation='v',
                     color='Total_Users',
                     color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=True)


if selected_tab == "Explore Data":
    st.markdown("<h1 style='text-align: center; color: #3E2A80; text-transform:uppercase'>Further exploration of available information</h1>", unsafe_allow_html=True)
    option = st.selectbox("Questions",[
    "Transactions of top 10 states","Transactions of top 10 district","Top 10 statewise users","Top 10 statewise app opens","Top 10 statewise users and app opens","Brand-Wise Sales Analysis"
    ,"Brand-Wise Percentage of Sales Analysis","District-wise users","District-wise app opens", "District-wise users and app opens"],
    index=None,
    placeholder="Select...")
    if option == 'Transactions of top 10 states':
        cursor.execute(f"select state, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from Aggr_trans group by state order by Total desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
        fig = px.pie(df, values='Total_Amount',
                            names='State',
                            color_discrete_sequence=px.colors.sequential.Agsunset,
                            hover_data=['Transactions_Count'],
                            labels={'Transactions_Count':'Transactions_Count'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)

    elif option == 'Transactions of top 10 district':
        cursor.execute(f"select district , sum(Transaction_count) as Total_Count, sum(transaction_amount) as Total from Map_trans group by district order by Total desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Transactions_Count','Total_Amount'])

        fig = px.pie(df, values='Total_Amount',
                            names='District',
                            color_discrete_sequence=px.colors.sequential.Agsunset,
                            hover_data=['Transactions_Count'],
                            labels={'Transactions_Count':'Transactions_Count'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)     
    
    elif option == 'Top 10 statewise users':
        cursor.execute(f"select state, sum(Registeredusers) as Total_Users from Map_user group by state order by Total_Users desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Users'])
        fig = px.pie(df, values='Total_Users',names='State',color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Users'], labels={'Total_Users':'Total_Users'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)

    elif option == 'Top 10 statewise app opens': ##bala
        cursor.execute(f"select state, sum(AppOpens) as Total_Appopens from Map_user group by state order by Total_Appopens desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['State','Total_Appopens'])
        fig = px.pie(df, values='Total_Appopens',names='State',color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Appopens'], labels={'Total_Appopens':'Total_Appopens'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)

    elif option == 'Top 10 statewise users and app opens':
        cursor.execute(f"select state, sum(Registeredusers) as Total_Users, sum(AppOpens) as Total_Appopens from Map_user group by state order by Total_Users desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
        fig = px.pie(df, values='Total_Users',names='State',color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Appopens'], labels={'Total_Appopens':'Total_Appopens'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)

    elif option == "Brand-Wise Sales Analysis":    
        cursor.execute(f"select brand, sum(count) as Total_Count from Aggr_user group by brand order by Total_Count desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['Brand', 'Total_Users'])
        fig = px.bar(df,x="Total_Users",
                        y="Brand",
                        orientation='h',
                        color='Total_Users',
                        color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=True)  

    elif option == "Brand-Wise Percentage of Sales Analysis":
        cursor.execute(f"select brand, sum(count) as Total_Count, avg(percentage)*100 as Avg_Percentage from Aggr_user group by brand order by Total_Count desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['Brand', 'Total_Users','Avg_Percentage'])
        fig = px.bar(df, x="Total_Users",
                    y="Brand",
                    orientation='h',
                    color='Avg_Percentage',
                    color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=True) 
    
    elif option == "District-wise users":
        st.markdown(f"<h5 style='color: #3E2A80;'>District</h5>", unsafe_allow_html=True)
        cursor.execute(f"select district, sum(registeredUsers) as Total_Users from Map_user group by district order by Total_Users desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Total_Users'])
        df.Total_Users = df.Total_Users.astype(float)
        fig = px.bar(df, x="Total_Users",
                        y="District",
                        orientation='h',
                        color='Total_Users',
                        color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=True)

    elif option == "District-wise app opens":
        st.markdown(f"<h5 style='color: #3E2A80;'>District</h5>", unsafe_allow_html=True)
        cursor.execute(f"select district, sum(AppOpens) as Total_Appopens from Map_user group by district order by Total_Appopens desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Total_Appopens'])
        df.Total_Users = df.Total_Appopens.astype(float)
        fig = px.bar(df,x="Total_Appopens",
                        y="District",
                        orientation='h',
                        color='Total_Appopens',
                        color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=True)

    elif option == "District-wise users and app opens":
        st.markdown(f"<h5 style='color: #3E2A80;'>District</h5>", unsafe_allow_html=True)
        cursor.execute(f"select district, sum(registeredUsers) as Total_Users, sum(AppOpens) as Total_Appopens from Map_user group by district order by Total_Users desc limit 10")
        df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Total_Users','Total_Appopens'])
        df.Total_Users = df.Total_Users.astype(float)
        fig = px.bar(df,x="Total_Users",
                        y="District",
                        orientation='h',
                        color='Total_Users',
                        color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig,use_container_width=True)