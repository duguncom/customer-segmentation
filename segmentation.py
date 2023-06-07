import sys
import os
import datetime
import numpy as np
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import src.utils as utils
import src.config as config
from pathlib import Path


#read our dataset
rfm_data = pd.read_csv(os.path.join("data","potentials.csv"))
payment = pd.read_csv(os.path.join("data","payment.csv"))
satisfaction=pd.read_csv(os.path.join("data","satisfaction.csv"))

# Boolean condition to filter rows
condition = (rfm_data['is_promotion'] != 1) & (rfm_data['current_products_price'] > 0) & (rfm_data['membership_length'] > 0)

# Filter the DataFrame based on the condition
rfm_data = rfm_data[condition].copy()

#filling the NaN budget_value with 1
rfm_data['budget_value'].fillna(1, inplace=True)

# Sort the satisfaction dataframe by date in descending order
satisfaction_sorted = satisfaction.sort_values('satisfaction_date', ascending=False)

# Drop duplicate rows based on customer_id and provider_id, keeping only the first occurrence (which will be the latest date)
satisfaction_latest = satisfaction_sorted.drop_duplicates(['customer_id', 'provider_id'])

# Merge the dataframes on provider_id and customer_id using a left join
merged_df = rfm_data.merge(satisfaction[['provider_id', 'customer_id', 'value']], on=['provider_id', 'customer_id'], how='left')

# Add the satisfaction_status column to rfm_data_subs
rfm_data['satisfaction_status'] = merged_df['value']

# Group the payment dataframe by customer_id and get the latest payment_status_id for each customer
latest_payment_status = payment.groupby('customer_id')['payment_status_id'].last()

# Create a new column 'payment_status_id' in rfm_dataset and fill it with the latest payment_status_ids
rfm_data['payment_status_id'] = rfm_data['customer_id'].map(latest_payment_status)

#we will count information until today
reference_date = datetime.datetime.today().date()

# creating extra columns
rfm_data['days_since_last_call'] = (pd.to_datetime(reference_date) - pd.to_datetime(rfm_data['last_call'])).astype('timedelta64[D]')
rfm_data['days_since_last_touch'] = (pd.to_datetime(reference_date) - pd.to_datetime(rfm_data['last_touch'])).astype('timedelta64[D]')
rfm_data['days_since_last_seen'] = (pd.to_datetime(reference_date) - pd.to_datetime(rfm_data['last_seen_at'])).astype('timedelta64[D]')

# Fill NaN values in 'days_since_last_call' with the maximum value from the column
max_last_call = rfm_data['days_since_last_call'].max()
rfm_data['days_since_last_call'].fillna(max_last_call, inplace=True)

# Fill NaN values in 'days_since_last_touch' with the maximum value from the column
max_last_touch = rfm_data['days_since_last_touch'].max()
rfm_data['days_since_last_touch'].fillna(max_last_touch, inplace=True)

columns_to_normalize = ['budget_value', 'lead_read_gap_min', 'lead_count','view_count','image_count','video_count','discount_count','review_count','touch_count','call_count']  # List of columns to normalize

for column in columns_to_normalize:
    min_val = rfm_data[column].min()
    max_val = rfm_data[column].max()
    rfm_data[column] = (rfm_data[column] - min_val) / (max_val - min_val)

rfm_data = rfm_data[['provider_id', 'customer_id','lead_count','view_count','image_count','video_count','discount_count','review_count','touch_count','call_count',
                    'membership_length','budget_value','current_products_price','lead_read_gap_min',
                    'days_since_last_call', 'days_since_last_touch','days_since_last_seen','satisfaction_status','payment_status_id']]

# Function to calculate Monetary column
def calculate_monetary(df):
    """
    this function will be used to create a monetary score and assign it to a seperate column created and named as Monetary
    """
    df['Monetary'] = df['current_products_price'] * df['budget_value']

# Function to calculate Frequency column
def calculate_frequency(df):
    """
    this function will be used to create a frequency score and assign it to a seperate column created and named as Frequency
    """
    df['Frequency'] = (df['image_count'] + df['video_count'] + df['discount_count'] + df['review_count'] + df['lead_count'] + df['view_count'] +
                      (1.5 * (df['touch_count'] + df['call_count']))) / df['membership_length']

# Function to calculate Recency column
def calculate_recency(df):
    """
    this function will be used to create a recency score and assign it to a seperate column created and named as Recency
    """
    min_last_touch = df['days_since_last_touch'].min()
    min_last_seen = df['days_since_last_seen'].min()
    min_last_call = df['days_since_last_call'].min()

    df['Recency'] = np.minimum.reduce([min_last_touch, min_last_seen, min_last_call]) * df['lead_read_gap_min']

# Calculate the columns using the defined functions
calculate_monetary(rfm_data)
calculate_frequency(rfm_data)
calculate_recency(rfm_data)

#now let's create our final RFM dataset to evaluate :
rfm_providers=rfm_data[["provider_id"]]
rfm_customers= rfm_data[["customer_id"]]
rfm_satisfaction=rfm_data[["satisfaction_status"]]
rfm_payment=rfm_data[["payment_status_id"]]
rfm_data = rfm_data[["Recency","Monetary","Frequency"]]

quantiles = rfm_data.quantile(q=[0.25,0.5,0.75])
quantiles.to_dict()

def RScore(x,p,d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]: 
        return 2
    else:
        return 1
def FMScore(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]: 
        return 3
    else:
        return 4

rfm_segmentation = rfm_data
rfm_segmentation['R_Quartile'] = rfm_segmentation['Recency'].apply(RScore, args=('Recency',quantiles,))
rfm_segmentation['F_Quartile'] = rfm_segmentation['Frequency'].apply(FMScore, args=('Frequency',quantiles,))
rfm_segmentation['M_Quartile'] = rfm_segmentation['Monetary'].apply(FMScore, args=('Monetary',quantiles,))

rfm_segmentation['RFMScore'] = rfm_segmentation.R_Quartile.map(str) \
                            + rfm_segmentation.F_Quartile.map(str) \
                            + rfm_segmentation.M_Quartile.map(str)

rfm_segmentation['RFMScore_num'] = rfm_segmentation.R_Quartile \
                            + rfm_segmentation.F_Quartile \
                            + rfm_segmentation.M_Quartile

dfs=[rfm_providers,rfm_customers,rfm_satisfaction,rfm_payment,rfm_segmentation]
for df in dfs:
    df.reset_index(drop=True, inplace=True)

merged_df = dfs[0]

# Merge the remaining dataframes on index one by one
for df in dfs[1:]:
    merged_df = pd.merge(merged_df, df, left_index=True, right_index=True)

#Saving the feature engineering results as CSV file
merged_df.to_csv(Path(os.getcwd(),"data","rfm_segmentation.csv"),index=False)
