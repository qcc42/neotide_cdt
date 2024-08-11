import sqlalchemy as sqla
import csv
import pandas as pd
import datetime as dt

array = []

with open("data.csv") as file:
    reader = csv.reader(file, delimiter = ' ')

    for row in reader:
        array.append(row)
array_avd = []
with open("data_avd.csv") as file:
    reader = csv.reader(file, delimiter = ' ')

    for row in reader:
        array_avd.append(row)


df = pd.DataFrame({'name': [i[1] for i in array], 'date' : [dt.date.fromisoformat(i[2]) for i in array], 'dept_no' : [i[3] for i in array]}, index = [i[0] for i in array])
df.index.name = 'patient_no'

df_avd = pd.DataFrame( {'name': [i[1] for i in array_avd]} ,index = [i[0] for i in array_avd])
df_avd.index.name = 'dept_no'

engine = sqla.create_engine('sqlite://')
df.to_sql(name = 'temp_patients', con = engine, dtype = {'name': sqla.String, 'date': sqla.Date, 'patient_no':  sqla.String})
df_avd.to_sql(name = 'temp_avd', con = engine, dtype = {'name': sqla.String, 'dept_no': sqla.String})
with engine.connect() as connection:

    connection.execute(sqla.text("CREATE TABLE departments ( dept_no STRING, name STRING, primary key (dept_no))"))
    connection.execute(sqla.text("INSERT INTO departments SELECT * FROM temp_avd;"))
    connection.execute(sqla.text("PRAGMA foreign_keys = ON;"))
    connection.execute(sqla.text("CREATE TABLE patients ( patient_no STRING, name STRING, date DATE, dept_no STRING , primary key (patient_no, date) foreign key (dept_no) REFERENCES departments(dept_no))"))
    connection.execute(sqla.text("INSERT INTO patients SELECT * FROM temp_patients;"))

    print(connection.execute(sqla.text("SELECT patients.name, departments.name FROM patients JOIN departments ON patients.dept_no = departments.dept_no OR departments.dept_no = NULL;")).fetchall())

