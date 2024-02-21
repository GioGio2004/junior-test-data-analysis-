import pandas as pd
from datetime import timedelta

# loading the data from xcele files 
orders_df = pd.read_excel('testingdata/test-orders.xlsx')
currency_rates_df = pd.read_excel('testingdata/test-currency-rates.xlsx')
affiliate_rates_df = pd.read_excel('testingdata/test-affiliate-rates.xlsx')

# searching for duplicates in this databases 
dataframes = [(orders_df, "Orders"), (currency_rates_df, "Currency Rates"), (affiliate_rates_df, "Affiliate Rates")]

# looping through datasets to identify douplicates and missing vaclues
for df, df_name in dataframes:
    for column in df.columns:
        length_of_column = len(df[column])
        number_of_null_values = 0
        
        for i in range(length_of_column):
            if pd.isnull(df.loc[i, column]):
                number_of_null_values += 1
                print(f"Row {i} in '{df_name}'  dataset contains a null value in '{column}' column")

        print(f"'{df_name}' datasets column {column}' contains '{number_of_null_values}' null values")

# droping duplicate values and cleaning
orders_df.drop_duplicates(inplace=True)
currency_rates_df.drop_duplicates(inplace=True)
affiliate_rates_df.drop_duplicates(inplace=True)

# droping typoes and missed values
orders_df.fillna(value=0, inplace=True) 
currency_rates_df.dropna(inplace=True) 

# converting order amount into EUR courrency
merged_df = pd.merge(orders_df, currency_rates_df, left_on='Order Date', right_on='date', how='left')
merged_df['Order Amount (EUR)'] = merged_df['Order Amount'] / merged_df['USD']

# Calculate fees for each order
merged_df['Week Start'] = merged_df['Order Date'] - pd.to_timedelta(merged_df['Order Date'].dt.dayofweek, unit='D')
merged_df = merged_df.merge(affiliate_rates_df, on='Affiliate ID')
merged_df['Processing Fee'] = merged_df['Order Amount (EUR)'] * merged_df['Processing Rate']
merged_df['Refund Fee'] = merged_df['Refund Fee'].where(merged_df['Order Status'] == 'Refunded', 0)
merged_df['Chargeback Fee'] = merged_df['Chargeback Fee'].where(merged_df['Order Status'] == 'Chargeback', 0)

# generateing weekly aggregation for each affiliate and saveing it into Excel fiel 

for affiliate_id, aff_data in merged_df.groupby('Affiliate ID'):
    affiliate_name = aff_data['Affiliate Name'].iloc[0]
    weekly_agg = aff_data.groupby(pd.Grouper(key='Order Date', freq='W-SUN')).agg({
        'Order Number': 'count',
        'Order Amount (EUR)': 'sum',
        'Processing Fee': 'sum',
        'Refund Fee': 'sum',
        'Chargeback Fee': 'sum'
    }).reset_index()
    weekly_agg['Week'] = weekly_agg['Order Date'].dt.strftime('%d-%m-%Y - ') + (weekly_agg['Order Date'] + timedelta(days=6)).dt.strftime('%d-%m-%Y')
    weekly_agg.drop(columns=['Order Date'], inplace=True)
    weekly_agg.rename(columns={'Order Number': 'Number of Orders', 'Order Amount (EUR)': 'Total Order Amount (EUR)'}, inplace=True)
    excel_file_name = f"{affiliate_name}.xlsx"
    weekly_agg.to_excel(excel_file_name, index=False)
    print(f"Excel report for {affiliate_name} saved as {excel_file_name}")