import sqlalchemy as sqla
import csv
import pandas as pd
import datetime as dt
import unittest as ut

class TestMain(ut.TestCase):

    def test_pk(self):
        array = []
        with open("data.csv") as file:
            reader = csv.reader(file, delimiter=' ')

            for row in reader:
                array.append(row)
        array[0][0] = array[1][0] #duplicating entries to verify that primary key duplication does not work
        array[0][2] = array[1][2]
        df = pd.DataFrame({'name': [i[1] for i in array], 'date': [dt.date.fromisoformat(i[2]) for i in array],
                           'dept_no': [i[3] for i in array]}, index=[i[0] for i in array])
        df.index.name = 'patient_no'

        engine = sqla.create_engine('sqlite://')
        df.to_sql(name='temp_patients', con=engine,
                  dtype={'name': sqla.String, 'date': sqla.Date, 'patient_no': sqla.String})

        with engine.connect() as connection:
            connection.execute(sqla.text("CREATE TABLE patients ( patient_no STRING, name STRING, date DATE, dept_no STRING , primary key (patient_no, date))"))
            with self.assertRaises(sqla.exc.IntegrityError): #here we assert that there is an error, because two entities have the same primary key
                connection.execute(sqla.text("INSERT INTO patients SELECT * FROM temp_patients;"))
    def test_fk(self):

        array = []
        with open("data.csv") as file:
            reader = csv.reader(file, delimiter=' ')

            for row in reader:
                array.append(row)
        df = pd.DataFrame({'name': [i[1] for i in array], 'date': [dt.date.fromisoformat(i[2]) for i in array],
                           'dept_no': [i[3] for i in array]}, index=[i[0] for i in array])
        df.index.name = 'patient_no'

        engine2 = sqla.create_engine('sqlite://')
        df.to_sql(name='temp_patients', con=engine2,
                  dtype={'name': sqla.String, 'date': sqla.Date, 'patient_no': sqla.String})

        array_avd = []
        with open("data_avd.csv") as file:
            reader = csv.reader(file, delimiter=' ')
            for row in reader:
                array_avd.append(row)

        array_avd.pop() #this should result in integrity error, since one of the departments are missing

        df_avd = pd.DataFrame({'name': [i[1] for i in array_avd]}, index=[i[0] for i in array_avd])
        df_avd.index.name = 'dept_no'
        df_avd.to_sql(name='temp_avd', con=engine2, dtype={'name': sqla.String, 'dept_no': sqla.String})
        with engine2.connect() as connection:
            connection.execute(sqla.text("PRAGMA foreign_keys = ON;"))
            connection.execute(sqla.text("CREATE TABLE departments ( dept_no STRING, name STRING, primary key (dept_no))"))
            connection.execute(sqla.text("INSERT INTO departments SELECT * FROM temp_avd;"))
            connection.execute(sqla.text("CREATE TABLE patients ( patient_no STRING, name STRING, date DATE, dept_no STRING , primary key (patient_no, date) foreign key (dept_no) REFERENCES departments(dept_no))"))
            with self.assertRaises(sqla.exc.IntegrityError): #here we assert that there is a fact an error
                connection.execute(sqla.text("INSERT INTO patients SELECT * FROM temp_patients;"))

if __name__ == '__main__':
    ut.main()