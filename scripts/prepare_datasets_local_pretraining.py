import os
import pandas as pd
from sklearn.model_selection import train_test_split

datasets_folder = "../datasets/"
csv_files = ["Fraud.csv", "Credit_Card_Fraud_.csv", "transactions_df.csv"]


def split_and_save_csv(file_name, folder_path):
    file_path = os.path.join(folder_path, file_name)

    try:
        data = pd.read_csv(file_path)

        train, test = train_test_split(data, test_size=0.3, random_state=42)

        test_file = os.path.join(folder_path, f"test_{file_name}")
        train_file = os.path.join(folder_path, f"train_{file_name}")
        train.to_csv(train_file, index=False)
        test.to_csv(test_file, index=False)

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")


for csv_file in csv_files:
    split_and_save_csv(csv_file, datasets_folder)
