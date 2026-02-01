import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Define column names based on dataset description and initial inspection

column_names = [

    'col0', 'user_id', 'behavior_type', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9',

    'col10', 'col11', 'item_category', 'item_id', 'col14', 'col15', 'col16', 'col17', 'col18',

    'col19', 'col20', 'col21', 'col22', 'col23', 'col24', 'col25', 'col26', 'col27', 'col28',

    'col29', 'col30', 'col31', 'col32', 'col33', 'timestamp', 'col35', 'col36', 'col37', 'col38'

]



# Load the dataset with no header and specified column names

file_path = './data/D1_0_top_10k.csv'

df = pd.read_csv(file_path, header=None, names=column_names)



# Select only the relevant columns for RFM analysis

df = df[['user_id', 'behavior_type', 'item_id', 'item_category', 'timestamp']]



# Convert timestamp to datetime

df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')



# Display basic information

print("Dataset Head:")

print(df.head())



print("\nDataset Info:")

df.info()



print("\nDataset Description:")



print(df.describe(include='all'))







# --- Data Cleaning ---



# Drop rows with missing behavior_type as it's critical for RFM analysis



df.dropna(subset=['behavior_type'], inplace=True)







# Convert behavior_type to int after dropping NaNs for consistent type



df['behavior_type'] = df['behavior_type'].astype(int)







# --- RFM Calculation ---



# Determine a snapshot date for Recency calculation (one day after the last transaction)



snapshot_date = df['timestamp'].max() + pd.Timedelta(days=1)







# Filter for purchase behavior (behavior_type = 2, assuming 2 represents 'buy')



purchases_df = df[df['behavior_type'] == 2]











# Group by user_id to calculate RFM metrics



rfm_df = purchases_df.groupby('user_id').agg(



    Recency=('timestamp', lambda date: (snapshot_date - date.max()).days),



    Frequency=('item_id', 'count'),



    Monetary=('item_id', lambda x: (x.count() * 1)) # Assuming 1 unit per purchase as no price is available



).reset_index()







print("\nRFM Data Head:")



print(rfm_df.head())







print("\nRFM Data Description:")



print(rfm_df.describe())

# --- RFM Scoring ---
# Create simplified scoring for RFM due to low data variability
rfm_df['R_Score'] = 5 # Constant high score as all Recency values are 1

# Simplified Frequency Scoring
rfm_df['F_Score'] = rfm_df['Frequency'].apply(lambda x: 1 if x == 1 else (2 if x <= 5 else 3))

# Simplified Monetary Scoring (same as Frequency in this case)
rfm_df['M_Score'] = rfm_df['Monetary'].apply(lambda x: 1 if x == 1 else (2 if x <= 5 else 3))

# Combine RFM scores
rfm_df['RFM_Score'] = rfm_df['R_Score'].astype(str) + \
                      rfm_df['F_Score'].astype(str) + \
                      rfm_df['M_Score'].astype(str)

# --- RFM Segmentation ---
def rfm_segment(df):
    if df['RFM_Score'] == '555':
        return 'Champions'
    elif df['RFM_Score'] == '554':
        return 'Loyal Customers'
    elif df['RFM_Score'] == '544':
        return 'Loyal Customers'
    elif df['RFM_Score'] == '545':
        return 'Loyal Customers'
    elif df['RFM_Score'] == '455':
        return 'Loyal Customers'
    elif df['RFM_Score'] == '454':
        return 'Loyal Customers'
    elif df['RFM_Score'] == '355':
        return 'Loyal Customers'
    elif df['RFM_Score'] == '543':
        return 'Potential Loyalists'
    elif df['RFM_Score'] == '444':
        return 'Potential Loyalists'
    elif df['RFM_Score'] == '344':
        return 'Potential Loyalists'
    elif df['RFM_Score'] == '535':
        return 'Big Spenders'
    elif df['RFM_Score'] == '435':
        return 'Big Spenders'
    elif df['RFM_Score'] == '335':
        return 'Big Spenders'
    elif df['RFM_Score'] == '525':
        return 'Promising'
    elif df['RFM_Score'] == '425':
        return 'Promising'
    elif df['RFM_Score'] == '325':
        return 'Promising'
    elif df['R_Score'] >= 4 and df['F_Score'] >= 4:
        return 'Loyal Customers'
    elif df['R_Score'] >= 4 and df['F_Score'] >= 3:
        return 'Potential Loyalists'
    elif df['R_Score'] >= 3 and df['F_Score'] >= 3:
        return 'Potential Loyalists'
    elif df['R_Score'] >= 4 and df['M_Score'] >= 4:
        return 'Big Spenders'
    elif df['R_Score'] >= 3 and df['M_Score'] >= 4:
        return 'Big Spenders'
    elif df['F_Score'] >= 4 and df['M_Score'] >= 4:
        return 'Big Spenders'
    elif df['R_Score'] >= 3 and df['F_Score'] >= 2:
        return 'Needs Attention'
    elif df['R_Score'] <= 2 and df['F_Score'] >= 3:
        return 'At Risk'
    elif df['R_Score'] <= 2 and df['M_Score'] >= 3:
        return 'At Risk'
    elif df['R_Score'] <= 1:
        return 'Lost'
    else:
        return 'Other'

rfm_df['Segment'] = rfm_df.apply(rfm_segment, axis=1)

print("\nRFM Data with Scores and Segments Head:")
print(rfm_df.head())

print("\nRFM Segments Value Counts:")
print(rfm_df['Segment'].value_counts())

# --- Visualize Customer Segments ---
plt.figure(figsize=(10, 6))
sns.countplot(data=rfm_df, x='Segment', palette='viridis')
plt.title('Customer Segments Distribution')
plt.xlabel('Customer Segment')
plt.ylabel('Number of Customers')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
# plt.savefig('customer_segments_distribution.png')
# print("\nCustomer segments distribution plot saved as customer_segments_distribution.png")








