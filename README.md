# Big-Data-Analytics

## How to run

1. Make sure you have docker, Python (preferably 3.12.7), and OPTIONALLY JDK 17 (look at the step below; it is necessary for local model training with Spark) installed
1. NOTE: It is only needed for local model training; models are attached to the repository in the 'services/streaming_processing/models' folder, so you don't need to perform this step: Install winutils (if you are running on Windows) for Hadoop - https://github.com/steveloughran/winutils/ - place them in C:/hadoop/bin/  Add this folder to PATH and set HADOOP_HOME=C:/hadoop 
2. Start docker desktop
3. Download the datasets pointed out below
3. Install the requirements in requirements.txt
4. Run the notebooks in the 'eda' folder to preprocess the datasets
7. Modify config files if needed - look at the relevant files in the 'services' folder. The default configuration should work out of the box
5. Navigate to the 'scripts' folder
6. Run prepare_datasets_local_pretraining.py and train_local_model.py (only if you want to recreate the pre-trained models)
8. Run start_containers.bat (alternatively, one can run only certain scenarios such as streaming processing flow, data ingestion to hive, or batch processing; all scripts are available in the 'scripts' folder)
9. After a few minutes, run the post_start.bat in the 'scripts' folder. It will create Hive tables.
10. You should be able to access the services (look at docker-compose for ports and addresses; look at authorization-access-data.json for credentials)
11. To display the dashboards, navigate to http://localhost:8088/superset and use the connection string: trino://admin:@presto:8080/cassandra/fraud_analytics  Then run the 'import_superset_dashboards.bat' in 'scripts' folder

## Data

Download from:

1. https://www.kaggle.com/datasets/chitwanmanchanda/fraudulent-transactions-data

2. https://www.openml.org/search?type=data&status=active&id=45955&sort=runs

3. https://www.kaggle.com/datasets/cgrodrigues/credit-card-transactions-synthetic-data-generation?select=transactions_df.csv

And place unpacked files in the root folder in the 'datasets' folder.

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
