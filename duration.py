import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime, date

# provide date
parameter=input('Wprowadź datę YYYYMMDD: ')
date = parameter

# connect to MS SQL SERVER
server = '****************'
database = '***************'
username = '**********'
password = '******************'
driver= '{ODBC Driver 13 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+
                      ';SERVER='+server+
                      ';DATABASE='+database+
                      ';UID='+username+
                      ';PWD='+password)

# get data
SQL_Query = pd.read_sql_query(
    "SELECT * FROM [RiskDB].[bonds].[Obligacje] order by id asc", cnxn)
obligacje = pd.DataFrame(SQL_Query)

SQL_Query = pd.read_sql_query(
    "SELECT * FROM [RiskDB].[bonds].[pozycja] order by IdObligacje asc", cnxn)
pozycja = pd.DataFrame(SQL_Query)

SQL_Query = pd.read_sql_query(
    "SELECT * FROM [RiskDB].[bonds].[odsetki] order by IdObligacje asc, OkresDo asc", cnxn)
odsetki = pd.DataFrame(SQL_Query)

cnxn.close()



# def time to cf
def time_to_cf(id):

    strptime_date = datetime.strptime(date, '%Y%m%d').date()
    time_to_cf_list = []
    for index, row in odsetki.iterrows():
        if row['idObligacje'] == id:
            delta = row['DataWyplaty'] - strptime_date
            time_to_cf = delta.days/365
            time_to_cf_list.append(time_to_cf)


    # if no coupon in odsetki
    if len(time_to_cf_list) == 0:
        for index, row in obligacje.iterrows():
            if row['id'] == id:
                ISIN = row['ISIN']
                Id = row['id']
                wina_Tuska = 'błąd: Brak zapisu w tabeli ODSETKI dla obligacji: idObligacje: {}, ISIN: {}.'.format(Id, ISIN)
                print(wina_Tuska)

    else:
        return time_to_cf_list


# def get face value
def get_face_value(index):
    FaceValue = obligacje['Nominal'].iloc[index]
    return FaceValue


# def get current price
def current_price(id):
    strptime_date = datetime.strptime(date, '%Y%m%d').date()
    dates = []

    for index, row in pozycja.iterrows():
        if row['idObligacje'] == id:
            dates.append(row['Data'])

    # if no row 'wycena' for particular bond in POZYCJA:
    if len(dates) == 0:
        for index, row in obligacje.iterrows():
            if row['id'] == id:
                ISIN = row['ISIN']
                Id = row['id']
                blad = 'błąd: Brak wyceny w tabeli POZYCJA dla wskazanej obligacji: idObligacje: {}, ISIN: {}.'.format(
                    Id, ISIN)
        print(blad)
        # return blad

    # 'wycena' exists
    else:
        # get actual 'wycena'
        dates.sort()
        past_wycena = []
        for element in dates:
            if strptime_date >= element:
                past_wycena.append(element)

        # if no value 'wycena' for particular date
        if len(past_wycena) == 0:
            for index, row in obligacje.iterrows():
                if row['id'] == id:
                    date_list = []

                    for index2, row2 in pozycja.iterrows():
                        if row2['idObligacje'] == id:
                            date_list.append(row2['Data'])

                    date_range_1 = date_list[1].strftime('%Y/%m/%d')
                    date_range_2 = date_list[-1].strftime('%Y/%m/%d')
                    ISIN = row['ISIN']
                    Id = row['id']
                    blad1 = 'błąd: Brak wyceny obligacji: id: {}, ISIN: {} dla podanej daty: {}. ' \
                            'Wprowadź datę z zakresu od {}, do {}.'.format(Id, ISIN, strptime_date, date_range_1, date_range_2)
            print(blad1)
            # return blad1

        # value 'wycena' exists
        else:
            past_wycena.sort(reverse=True)
            data_wyceny = past_wycena[0]
            for index, row in pozycja.iterrows():
                if row['Data'] == data_wyceny and row['idObligacje'] == id:
                    wycena = row['Wycena'] / row['Liczba']
            return wycena


# def get coupon rates
def get_coupon_rates(id, face_value):
    try:
        coupon_rates_list = []
        for index, row in odsetki.iterrows():
            if row['idObligacje'] == id:
                coupon_rates_list.append(row['Odsetki'])
        coupon_rates_list[-1] = coupon_rates_list[-1] + face_value
        return coupon_rates_list
    except:
        return None


def cf_zdyskontowany(YTM, time_to_cf, CF, face_value=None, id=None, date=None):

    try:
        # if constant coupon
        if face_value is None and id is None and date is None:
            cf_zd = [CF[i] / (np.power(1 + YTM, time_to_cf[i])) for i in range(len(time_to_cf))]

        # mutable coupon
        else:
            strptime_date = datetime.strptime(date, '%Y%m%d').date()
            df = odsetki[(odsetki['idObligacje'] == id) & (strptime_date >= odsetki['OkresOd']) & (
                        strptime_date <= odsetki['OkresDo'])]
            CF = df['Odsetki'].values + face_value

            cf_zd = [CF / (np.power(1 + YTM, time_to_cf[i])) for i in range(len(time_to_cf))]

        return cf_zd

    except:
        return None



def find_YTM(time_to_cf, CF, current_pr, face_value=None, id=None, date=None):

    try:
        val_dict = {}
        for i in np.linspace(0.01, 0.03, 20001):
            val = np.sum(cf_zdyskontowany(YTM=i, time_to_cf=time_to_cf, CF=CF,
                                          face_value=face_value, id=id, date=date))
            val = np.around(val, decimals=4)
            val_dict[i] = val

        val_cf = [val_dict[i] for i in val_dict]

        number = min(val_cf, key=lambda x: abs(x - current_pr))

        for i in val_dict:
            if val_dict[i] == number:
                yield_to_maturity = i

        return yield_to_maturity

    except:
        return None


# mianownik
def mianownik(cf_zdyskontowany, time_to_cf):

    try:
        Mianownik = np.sum([cf_zdyskontowany[i] * time_to_cf[i] for i in range(len(cf_zdyskontowany))])

        return Mianownik

    except:
        return None


def duration(mianownik, cf_zdyskontowany):
    try:
        Duration = mianownik / np.sum(cf_zdyskontowany)
        return Duration
    except:
        return None


def mod_duration(duration, YTM):

    try:
        mod_d = duration / (1+YTM)
        return mod_d

    except:
        return None


test_dur_list= []

# calculate duration
for index, row in obligacje.iterrows():
    #     print(row['Nazwa'], row['ISIN'])
    values = []

    # get 'id' from 'obligacje' dataframe
    idObligacje = obligacje['id'].iloc[index]
    values.append(idObligacje)

    # calculate time to cf
    TimeToCf = time_to_cf(idObligacje)
    values.append(TimeToCf)

    # get face value
    FaceValue = get_face_value(index)
    values.append(FaceValue)

    # get current price
    CurrentPr = current_price(idObligacje)
    values.append(CurrentPr)

    # get coupon rates
    CouponRates = get_coupon_rates(id=idObligacje, face_value=FaceValue)
    values.append(np.sum(CouponRates))

    print('czekaj.....')

    # for constant coupon
    if row['StalyKupon'] == 1:

        # find YTM
        YTM = find_YTM(time_to_cf=TimeToCf, CF=CouponRates, current_pr=CurrentPr)
        values.append(YTM)

        # CF zdyskontowany
        CFZdykosntowany = cf_zdyskontowany(YTM=YTM, time_to_cf=TimeToCf, CF=CouponRates)
        values.append(np.sum(CFZdykosntowany))

        # mianownik
        Mianownik = mianownik(cf_zdyskontowany=CFZdykosntowany,
                              time_to_cf=TimeToCf)
        values.append(Mianownik)

        # duration
        Duration = duration(mianownik=Mianownik,
                            cf_zdyskontowany=CFZdykosntowany)
        values.append(Duration)

        # modified duration
        Mod_Duration = mod_duration(duration=Duration, YTM=YTM)
        values.append(Mod_Duration)

    else:
        # For mutable coupon

        # find YTM
        YTM = find_YTM(time_to_cf=TimeToCf, CF=CouponRates, current_pr=CurrentPr,
                       face_value=FaceValue, id=idObligacje, date=date)
        values.append(YTM)

        # CF zdyskontowany
        CFZdykosntowany = cf_zdyskontowany(YTM=YTM, time_to_cf=TimeToCf, CF=CouponRates,
                                           face_value=FaceValue, id=idObligacje, date=date)
        values.append(np.sum(CFZdykosntowany))

        # mianownik
        Mianownik = mianownik(cf_zdyskontowany=CFZdykosntowany,
                              time_to_cf=TimeToCf)
        values.append(Mianownik)

        # duration
        Duration = duration(mianownik=Mianownik,
                            cf_zdyskontowany=CFZdykosntowany)
        values.append(Duration)

        # modified duration
        Mod_Duration = mod_duration(duration=Duration, YTM=YTM)
        values.append(Mod_Duration)

    # print(values)

    if None in values:
        continue

    else:
        test_dur = pd.DataFrame({'idObligacje': [values[0]],
                                 # 'TimeToCF': [np.around(values[1], decimals=3)],
                                 'FaceValue': [values[2]],
                                 'CurrentPrice': [values[3]],
                                 'CashFlow': [values[4]],
                                 'YTM': [np.around(values[5], decimals=4)],
                                 'Duration': [np.around(values[8], decimals=3)],
                                 'ModDur': [np.around(values[9], decimals=3)],
                                 'DateDur': [date]
                                 }
                                )

        # print(test_dur)
        test_dur_list.append(test_dur)

# final dataframe
dur = pd.concat(test_dur_list)
# drop into excel
# dur.to_excel('duration_testowe.xlsx')

# connect to MS SQL SERVER
server = '*******************'
database = '****************'
username = '***************'
password = '***************'
driver= '{ODBC Driver 13 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+
                      ';SERVER='+server+
                      ';DATABASE='+database+
                      ';UID='+username+
                      ';PWD='+password)


# write data into MS SQL SERVER using stored procedure
idObligacje =dur['idObligacje'].values
# FaceValue = dur['FaceValue'].values
# CurrentPrice = dur['CurrentPrice'].values
CashFlow = dur['CashFlow'].values
YTM = dur['YTM'].values
Duration = dur['Duration'].values
ModDur = dur['ModDur'].values
DateDur = dur['DateDur'].values

for i in range(len(idObligacje)):

    sql = 'exec RiskDB.bonds.update_with_new_duration ?,?,?,?,?,?'
    val = (int(idObligacje[i]),
           # FaceValue[i],
           # CurrentPrice[i],
           CashFlow[i],
           YTM[i],
           Duration[i],
           ModDur[i],
           DateDur[i])

    cnxn.cursor().execute(sql, val)
    cnxn.cursor().commit()

cnxn.cursor().close()
cnxn.close()


print('='*40 + '  SUCCEED  ' + '='*40)



