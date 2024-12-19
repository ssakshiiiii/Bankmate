import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, DECIMAL, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the base class for declarative class definitions
Base = declarative_base()

# Define the class for the CustomerInformation table
class CustomerInformation(Base):
    __tablename__ = 'CustomerInformation'
    CustomerID = Column(Integer, primary_key=True)
    Name = Column(String(100))
    DateOfBirth = Column(Date)
    Gender = Column(String(10))
    Address = Column(String(255))
    City = Column(String(50))
    State = Column(String(50))
    PinCode = Column(String(10))
    DefaultStatus = Column(Integer)
    BankruptcyIndicator = Column(Integer)
    CreditInquiriesPast6Months = Column(Integer)
    MonthsSinceLastInquiry = Column(Integer)
    PercentageOpenAccounts = Column(DECIMAL(5, 2))
    PercentageOpenAccountsPast24Months = Column(DECIMAL(5, 2))
    TotalOpenAccounts = Column(Integer)
    TotalCreditLimitSum = Column(DECIMAL(10, 2))
    MaxCreditLimit = Column(DECIMAL(10, 2))
    PercentageHighBalanceAccounts = Column(DECIMAL(5, 2))
    PercentageSatisfiedAccounts = Column(DECIMAL(5, 2))
    BadDerogatoryMarksCount = Column(Integer)
    SatisfiedAccountsCount = Column(Integer)
    MonthsSinceFirstCreditAccount = Column(Integer)
    MonthsSinceLastCreditAccount = Column(Integer)
    DelinquentAccounts30To60DaysPast24Months = Column(Integer)
    DelinquentAccounts90DaysPast24Months = Column(Integer)
    TotalDelinquentAccounts60Days = Column(Integer)

# Define the class for the AccountInformation table
class AccountInformation(Base):
    __tablename__ = 'AccountInformation'
    AccountID = Column(Integer, primary_key=True)
    CustomerID = Column(Integer)
    AccountType = Column(String(50))
    AccountNumber = Column(String(50))
    AccountStatus = Column(String(50))
    Balance = Column(DECIMAL(10, 2))
    CreditLimit = Column(DECIMAL(10, 2))
    OverdraftLimit = Column(DECIMAL(10, 2))
    InterestRate = Column(DECIMAL(5, 2))
    DateOpened = Column(Date)
    DateClosed = Column(Date)
    MonthlyFee = Column(DECIMAL(10, 2))
    MinimumBalanceRequired = Column(DECIMAL(10, 2))
    LastTransactionDate = Column(Date)
    TotalDeposits = Column(DECIMAL(10, 2))
    TotalWithdrawals = Column(DECIMAL(10, 2))
    TotalTransactions = Column(Integer)


# Define the class for the TransactionInformation table
class TransactionInformation(Base):
    __tablename__ = 'TransactionInformation'
    TransactionID = Column(Integer, primary_key=True)
    AccountID = Column(Integer)
    TransactionDate = Column(Date)
    TransactionAmount = Column(DECIMAL(10, 2))
    TransactionType = Column(String(50))

# Define the class for the LoanInformation table
class LoanInformation(Base):
    __tablename__ = 'LoanInformation'
    LoanID = Column(Integer, primary_key=True)
    CustomerID = Column(Integer)
    Dependents = Column(Integer)
    Education = Column(String(50))
    LoanAmount = Column(DECIMAL(10, 2))
    LoanType = Column(String(50))
    LoanTerm = Column(String(50))
    Collateral = Column(String(50))
    LoanStatus = Column(String(50))
    Applicant_income = Column(Integer)
    Coapplicant_income = Column(Integer)

# Define the class for the CreditCardInformation table
class CreditCardInformation(Base):
    __tablename__ = 'CreditCardInformation'
    CreditCardID = Column(Integer, primary_key=True)
    CustomerID = Column(Integer)
    CreditLimit = Column(DECIMAL(10, 2))
    Balance = Column(DECIMAL(10, 2))
    CreditCardStatus = Column(String(50))
    CreditCardApplicationDate = Column(Date)

# Create the SQLite engine
engine = create_engine('sqlite:///datawarehouse.db')

# Create the tables in the database
Base.metadata.create_all(engine)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)
# Create a session
session = Session()

# Function to download data from a URL
def download_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

# Function to load data into a table
def load_data_to_table(data, table_class):
    df = pd.read_csv(pd.compat.StringIO(data.decode('utf-8')))
    df.to_sql(table_class.__tablename__, con=engine, if_exists='append', index=False)

# Function to fetch and load data
def fetch_and_load_data(url, table_class):
    data = download_data(url)
    load_data_to_table(data, table_class)

# ETL Process for each table
def etl_customer_information(url):
    fetch_and_load_data(url, CustomerInformation)

def etl_account_information(url):
    fetch_and_load_data(url, AccountInformation)

def etl_transaction_information(url):
    fetch_and_load_data(url, TransactionInformation)

def etl_loan_information(url):
    fetch_and_load_data(url, LoanInformation)

def etl_credit_card_information(url):
    fetch_and_load_data(url, CreditCardInformation)

customer_info_url = 'https://CustomerInformation.csv'
account_info_url = 'https://AccountInformation.csv'
transaction_info_url = 'https://TransactionInformation.csv'
loan_info_url = 'https://LoanInformation.csv'
creditcard_info_url = 'https://CreditCardInformation.csv'

etl_customer_information(customer_info_url)
etl_account_information(account_info_url)
etl_transaction_information(transaction_info_url)
etl_loan_information(loan_info_url)
etl_credit_card_information(creditcard_info_url)
