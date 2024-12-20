# Big-Data-Analytics

## How to run

1. Make sure you have docker installed
2. Start docker desktop
3. Download datasets pointed below
4. Modify config files if needed
5. Navigate to 'scripts' folder and run start_containers.bat
6. After a couple of minutes, you should be able to access the services (look at docker-compose for ports and addresses, look at authorization-access-data.json for credentials)

## Data

Download from:

1. https://www.kaggle.com/datasets/chitwanmanchanda/fraudulent-transactions-data

2. https://www.openml.org/search?type=data&status=active&id=45955&sort=runs

3. https://www.kaggle.com/datasets/cgrodrigues/credit-card-transactions-synthetic-data-generation?select=transactions_df.csv

and preferably place unpacked files in the root folder in 'datasets' folder.

Expected structure:
```
.  
├── datasets  
│   ├── Fraud.csv // dataset 1: 'Fraudulent Transactions Data' from Kaggle  
│   └── Credit_Card_Fraud_.arff // dataset 2: 'Credit_Card_Fraud_' from OpenML  
│   └── 3 // dataset 3: 'Credit Card Transactions Synthetic Data Generation' from Kaggle  
│   |   └── customer_profiles_table.csv  
│   |   └── terminal_profiles_table.csv  
│   |   └── transactions_df.csv  
│   └── creditcard.csv // dataset 4: 'Credit Card Fraud Detection' from Kaggle (optional)  
├── ...  
```