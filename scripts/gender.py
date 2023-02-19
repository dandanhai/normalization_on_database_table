import pymysql
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read("../config.ini")

USERNAME = config.get("database", "USERNAME")
HOST_IP = config.get("database", "HOST_IP")
PASSWORD = config.get("database", "PASSWORD")
DATABASE = config.get("database", "DATABASE")

db = pymysql.connect(
    host=HOST_IP,
    user=USERNAME,
    password=PASSWORD,
    database=DATABASE
)
cursor = db.cursor()

cursor.execute("SELECT gender FROM customers;")
data = cursor.fetchall()
data = pd.DataFrame(data)

gender_series = data[0]
gender_series = gender_series.astype("category")
gender_distribution = gender_series.value_counts()
gender_distribution.to_csv("../display/gender.csv")

# print(data[0][:10])
clean_gender_list = []
for i in range(len(data[0])):
    if data[0][i] in ["M", "F"]:
        clean_gender_list.append(data[0][i])
    else:
        if data[0][i] in ["Male", "ชาย", "male", "Male ", "Men", "Man", "男"]:
            clean_gender_list.append("M")
        elif data[0][i] in ["Female", "หญิง", "female", "女性", "Woman", "Femail", "Femal", "女", "หญิง ", "Women", "W",
                            "Fm", "Femalr", "Female "]:
            clean_gender_list.append("F")
        else:
            clean_gender_list.append("Unknown")
            
print(len(clean_gender_list))
data["clean gender"] = clean_gender_list
data.to_csv("../display/gender_clean.csv")
