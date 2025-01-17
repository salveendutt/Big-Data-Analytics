# Big-Data-Analytics

## How to run

1. Make sure you have docker and Python (preferably 3.12.7) installed
2. Start docker desktop
3. Download datasets pointed below
4. Run the notebooks in 'eda' and 'ml_training' folders to preprocess the datasets. It is necessary to have Python installed for this step, preferably 3.13.x
7. Modify config files if needed - look at the relevant files in the 'services' folder. Default configuration should work out of the box.
5. Navigate to 'scripts' folder.
6. Run prepare_datasets_local_pretraining.py and train_local_model.py
8. Run start_containers.bat (alternatively one can run only certain scenarios such as streaming processing flow, data ingestion to hive or batch processing, all scripts are available in the 'scripts' folder)
9. After a couple of minutes run the post_start.bat in 'scripts' folder. It will create Hive tables.
10. You should be able to access the services (look at docker-compose for ports and addresses, look at authorization-access-data.json for credentials)

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
│   └── transactions_df.csv // dataset 3: 'Credit Card Transactions Synthetic Data Generation' from Kaggle  
│   └── customer_profiles_table.csv  
│   └── terminal_profiles_table.csv   
│   └── creditcard.csv // dataset 4: 'Credit Card Fraud Detection' from Kaggle (optional)  
├── ...  
```