import sqlalchemy as sqla
import pprint as pp
import pandas as pd
import datetime as dt

array = []

with open("data_real.csv") as file:
    df = pd.read_csv(file, dtype = {"patientnumber":str ,"arrivaldate":str ,"releasedate": str,"ward": str,"weight": float})

def splitter(s):
    split = s.split(".")
    for i in range(len(split)):
        split[i] = int(split[i])
    split[0], split[2] = split[2], split[0]
    return split
for i in range(df.shape[0]):
    df['arrivaldate'][i] = dt.date(*splitter(df['arrivaldate'][i]))

    to_split = df['releasedate'][i]
    if type(to_split) is str and len(to_split) > 0:
        df['releasedate'][i] = dt.date(*splitter(to_split))
    else:
        df['releasedate'][i] = None

df_new = df.drop_duplicates(subset = ['patientnumber', 'arrivaldate'])

engine = sqla.create_engine('sqlite://')
df_new.to_sql(index = False, name = 'temp_patients', con = engine, dtype = {'weight': sqla.Float,'ward': sqla.String, 'releasedate': sqla.Date, 'arrivaldate': sqla.Date, 'patientnumber':  sqla.String})
df.set_index('patientnumber')


with engine.connect() as connection:

    connection.execute(sqla.text("CREATE TABLE patients ( patientnumber CHAR, arrivaldate DATE, ward CHAR, releasedate DATE, weight FLOAT , primary key (patientnumber, arrivaldate));"))
    connection.execute(sqla.text("INSERT INTO patients SELECT * FROM temp_patients;"))

    print(pp.pprint(connection.execute(sqla.text("SELECT * from patients;")).fetchall()))

