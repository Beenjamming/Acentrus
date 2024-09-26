import pandas as pd 
from sqlalchemy import create_engine

servername = 'xxxxxxxxxxx' #replace with server name
PARS = create_engine(r'mssql+pymssql://'+servername+'/xxxxxxxxxxxxx') #replace with database name

MDP_query = '''SELECT [PROV_ID]
      ,[npi]
      ,[DEA_NUMBER]
      ,[HUBID]
      ,[lastname]
      ,[first]
      ,[Middle]
      ,[a]
      ,[b]
      ,[dep]
      ,[c]
      ,[d]
      ,[e]
      ,[f]
      ,[g]
      ,cast(replace([OFFICE_PHONE_NUM],'-','') as varchar) "OfficePhone"
      ,cast(replace([OFFICE_FAX_NUM],'-','') as varchar) "Fax"
  FROM [xxxxxxxxxx].[dbo].[Acentrus_MDP]''' #replace with database name

PAF_query = '''SELECT cast([PAT_ENC_CSN_ID] as varchar) CSN
      ,[a]
      ,[b]
      ,[c]
      ,[rxnum]
      ,[IDENTITY_ID]
  FROM [xxxxxxxxxxxxx].[dbo].[Acentrus_PAF]''' #replace with database name

PRX_query = '''select * from [xxxxxxxxxxxx].[dbo].[Acentrus_PRX] where rxnum is not null''' #replace with database name

SRX_query = '''SELECT cast([npi] as varchar) "NPI"
      ,[PHARMACY_NAME]
      ,[a]
      ,[b]
      ,cast([NCPDP_ID] as varchar) "NCPDP"
      ,[c]
      ,[d]
      ,[CITY]
      ,[ABBR]
      ,cast([ZIP_CODE] as varchar)
      ,cast(replace([PHONE_NUMBER],'-','') as varchar) Phone
      ,cast(replace([FAX_NUMBER],'-','') as varchar) Fax
  FROM [xxxxxxxxxxxxx].[dbo].[Acentrus_SRX]''' #replace with database name

PAY_query = '''SELECT cast([PlanID1] as varchar) "PlanID1"
      ,[PlanName1]
      ,[PayType]
      ,[Epp1_12]
      ,[PlanSegment]
  FROM [xxxxxxxxxxxx].[dbo].[Acentrus_PAY]''' #replace with database name

MDP = pd.read_sql(MDP_query, PARS, coerce_float=False)
PAF = pd.read_sql(PAF_query, PARS, coerce_float=False)
PRX = pd.read_sql(PRX_query, PARS, coerce_float=False)
SRX = pd.read_sql(SRX_query, PARS, coerce_float=False)
PAY = pd.read_sql(PAY_query, PARS, coerce_float=False)

PRX = PRX.drop(columns=['ORDER_MED_ID'])
#change qty, PtPay1 and PtPay2 to decimal with 2 decimal places
columns = ['qty','PtPay1','PtPay2']
for i in columns:
    PRX[i] = PRX[i].astype(float).round(2)

columns = ['PAT_ENC_CSN_ID','BD','zip','npi','phrnpi','dayssupply','BENEFIT_PLAN_ID','BENEFIT_PLAN_ID2']

for i in columns:
    PRX[i] = PRX[i].astype(str).replace('\.0', '', regex=True)

#pad MRN with leading zeros to 8 digits
PRX['IDENTITY_ID'] = PRX['IDENTITY_ID'].str.zfill(8)

MDP = MDP.fillna('')
PAF = PAF.fillna('')
PRX = PRX.fillna('')
SRX = SRX.fillna('')
PAY = PAY.fillna('')

MDP = MDP.replace('NULL', '')
PAF = PAF.replace('NULL', '')
PRX = PRX.replace('NULL', '')
SRX = SRX.replace('NULL', '')
PAY = PAY.replace('NULL', '')

MDP = MDP.replace('None', '')
PAF = PAF.replace('None', '')
PRX = PRX.replace('None', '')
SRX = SRX.replace('None', '')
PAY = PAY.replace('None', '')

path = 'xxxxxxxxxxxxxxxxxxx' #replace with path

MDP.to_csv(f'{path}MDP.txt', index=False, header=False, sep='|')
PAF.to_csv(f'{path}PAF.txt', index=False, header=False, sep='|')
PRX.to_csv(f'{path}PRX.txt', index=False, header=False, sep='|')
SRX.to_csv(f'{path}SRX.txt', index=False, header=False, sep='|')
PAY.to_csv(f'{path}PAY.txt', index=False, header=False, sep='|')