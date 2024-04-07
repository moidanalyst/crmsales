import pandas as pd
import numpy as np
import math

# Load Data from csv files
accounts_df = pd.read_csv("accounts.csv")
products_df = pd.read_csv("products.csv")
sales_df = pd.read_csv("sales_pipeline.csv")
teams_df = pd.read_csv("sales_teams.csv")

# Change the data types of columns in sales_df
sales_df["close_date"] = pd.to_datetime(sales_df["close_date"], format='%Y-%m-%d')
sales_df["engage_date"] = pd.to_datetime(sales_df["engage_date"], format='%Y-%m-%d')

# Add a new column for time taken for deal to close
sales_df["time_taken"] = sales_df["close_date"] - sales_df["engage_date"]

# Setting deal win and lost filter for sales dataframe
filw = sales_df["deal_stage"] == "Won"
fill = sales_df["deal_stage"] == "Lost"

# sales_agents by total closing values
agent_by_close = sales_df[sales_df["deal_stage"]=="Won"].groupby(['sales_agent'])['close_value'].sum().reset_index()
agent_by_close.index += 1

# Example of Pivot Tables in DataFrames
# pivot_table = counts.pivot(index='sales_agents', columns='deal_stage', values='count')

# sales_agents by deal stages
agent_by_deals = sales_df.pivot_table(index="sales_agent", columns="deal_stage", values="opportunity_id", aggfunc="count")
agent_by_deals.fillna(0, inplace=True)

agent_by_deals["Total"] = agent_by_deals.iloc[:,0] + agent_by_deals.iloc[:,1] + agent_by_deals.iloc[:,2] + agent_by_deals.iloc[:,3]

# Calculate Win Ratio of an agent
agent_by_deals["Win Ratio"] = agent_by_deals["Won"] / agent_by_deals["Total"] * 100
agent_by_deals = agent_by_deals.round().astype(int)

# Calculate Loss Ratio of an agent
agent_by_deals["Loss Ratio"] = agent_by_deals["Lost"] / agent_by_deals["Total"] * 100
agent_by_deals = agent_by_deals.round().astype(int)

# Calculate deals won for each product
products_by_deals_won = sales_df[filw].groupby(["product"])["opportunity_id"].count()
products_by_deals_lost = sales_df[fill].groupby(["product"])["opportunity_id"].count()

# Calculate closing values of deals won for each product
products_by_close = sales_df[filw].groupby(["product"])["close_value"].sum()

# Join these two series into a new dataframe
products_by_deals_won_lost = pd.concat([products_by_deals_won, products_by_deals_lost, products_by_close], axis=1)

# Optionally, you can specify column names for clarity
products_by_deals_won_lost.columns = ['deals_won', 'deals_lost', 'close_value']

# Calculate average time taken by each agent to close the deal
avg_time_to_close = sales_df[filw].groupby(["sales_agent"])["time_taken"].mean().reset_index()
avg_time_to_close.index += 1

# Change type of time_taken column to string and then split using delimiter to get days in number as int
avg_time_to_close['time_taken'] = avg_time_to_close['time_taken'].astype(str)
avg_time_to_close['time_taken'] = avg_time_to_close['time_taken'].str.split(' ').str[0].astype(int)

# Join individually created DataFrames into one useful DataFrame for analysis
combined = agent_by_close.merge(agent_by_deals.reset_index(), 
                                    how="left", 
                                    left_on=['sales_agent'], 
                                    right_on=['sales_agent']).loc[:, ['sales_agent', 'close_value', 'Lost', 'Won']]

combined = combined.merge(avg_time_to_close,
                          how="left",
                          left_on=['sales_agent'], 
                          right_on=['sales_agent'])

# Change the name of the column
combined.rename(columns={'time_taken':'avg_time'}, inplace=True)




