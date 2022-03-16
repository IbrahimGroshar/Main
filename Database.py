
import mysql.connector
from mysql.connector import errorcode
import os
import msvcrt as m
import csv
import string
import re

cnx = mysql.connector.connect(user='root',
                             password='0000',
                             host='localhost',
                             database='nftMarketplace'
)


cursor = cnx.cursor()
database_name = "nftMarketplace"


def create_database(cursor, name):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))
        print("Using database {}".format(name))
    except mysql.connector.Error as err:
        print("Failed to create database {}".format(err))
        exit(1)


def ifExists(name):
  try:
    cursor.execute("USE {}".format(name))
  except mysql.connector.Error as err:
    print("Database {} does not exist".format(name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, name)
        print("Database {} created succesfully.".format(name))
        cnx.database = name
    else:
        print(err)



def create_table_users(cursor):
    create_users = "CREATE TABLE `users` (" \
                 "  `walletAddress` varchar(200) NOT NULL," \
                 "  `ownedNfts` varchar(50)," \
                 "  `funds` decimal(100)," \
                 "  `password` varchar(250)," \
                 "  PRIMARY KEY (`WalletAddress`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table Users: ")
        cursor.execute(create_users)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")



def create_table_sales(cursor):
    create_sales ="CREATE TABLE `sales` (" \
                 "  `saleNumber` int(50) NOT NULL," \
                 "  `date` varchar(50)," \
                 "  `nftSold` varchar(80) NOT NULL," \
                 "  `buyer` varchar(200)," \
                 "  `seller` varchar(200)," \
                 "  `price` int(80)," \
                 "  PRIMARY KEY (`saleNumber`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table Sales: ")
        cursor.execute(create_sales)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")



def create_table_nfts(cursor):
    create_nfts ="CREATE TABLE `nfts` (" \
                 "  `nft` varchar(100) NOT NULL," \
                 "  `rarity` varchar(50)," \
                 "  `creator` varchar(80)," \
                 "  `owner` varchar(200) NOT NULL," \
                 "  `blockchain` varchar(50)," \
                 "  PRIMARY KEY (`nft`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table Nfts: ")
        cursor.execute(create_nfts)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")



def create_table_creators(cursor):
    create_creators ="CREATE TABLE `creators` (" \
                 "  `walletAddress` varchar(100) NOT NULL," \
                 "  `verified` varchar(50)," \
                 "  `createdNft` varchar(100)," \
                 "  PRIMARY KEY (`walletAddress`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table Creators: ")
        cursor.execute(create_creators)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


# create_table_users(cursor)
# create_table_sales(cursor)
# create_table_nfts(cursor)
# create_table_creators(cursor)

def inserting_users(cursor):
    file = open(os.getcwd() + "\\" + "Users.csv")
    reader = csv.reader(file, delimiter=",")

    next(reader)
    for row in reader:
        print(row)
        walletAddress = row[0]
        ownedNfts = row[1]
        funds = row[2]
        password = row[3]
        
        cursor.execute("INSERT INTO users (walletAddress, ownedNfts, funds, password) VALUES (%s, %s, %s, %s)",(walletAddress, ownedNfts, funds, password))
    cnx.commit()

# inserting_users(cursor)

def inserting_nfts(cursor):
    file = open(os.getcwd() + "\\" + "Nfts.csv")
    reader = csv.reader(file, delimiter=",")

    next(reader)
    for row in reader:
        print(row)
        nft = row[0]
        rarity = row[1]
        creator = row[2]
        owner = row[3]
        blockchain = row[4]
        
        cursor.execute("INSERT INTO nfts (nft, rarity, creator, owner, blockchain) VALUES (%s, %s, %s, %s, %s)",(nft,rarity,creator,owner,blockchain))
    cnx.commit()

# inserting_nfts(cursor)

def inserting_sales(cursor):
    file = open(os.getcwd() + "\\" + "Sales.csv")
    reader = csv.reader(file, delimiter=",")

    next(reader)
    for row in reader:
        print(row)
        saleNumber = row[0]
        date = row[1]
        nftSold = row[2]
        buyer = row[3]
        seller = row[4]
        price = row[5]
        
        cursor.execute("INSERT INTO sales (saleNumber, date, nftSold, buyer, seller, price) VALUES (%s, %s, %s, %s, %s, %s)",(saleNumber, date, nftSold, buyer, seller, price))
    cnx.commit()
    
# inserting_sales(cursor)


def inserting_creators(cursor):
    file = open(os.getcwd() + "\\" + "Creators.csv")
    reader = csv.reader(file, delimiter=",")

    next(reader)
    for row in reader:
        print(row)
        walletAddress = row[0]
        verified = row[1]
        createdNft = row[2]
        
        cursor.execute("INSERT INTO creators (walletAddress, verified, createdNft) VALUES (%s, %s, %s)",(walletAddress, verified, createdNft))
    cnx.commit()
    
# inserting_creators(cursor)



def listAllNfts(cursor):
    result = cnx.cursor()

    cursor.execute("SELECT nft FROM nfts")
    result = cursor.fetchall()
    print()
    print("Listing all nfts in the nft database: ")
    print()
    print("   Nft names")
    print("+----------------------------------------+")
    for nft in result:
        nft = str(nft)
        chars = re.escape(string.punctuation)
        print("|   {:<35}  |".format(re.sub(r"["+chars+"]", '', nft)))
    print("+----------------------------------------+")
    print()
    input("Press anything to return to the menu: ")
    menu()

# listAllNfts(cursor)

def ownerToNft(cursor):
    wallet = "'"+ str(input("Please provide the wallet address: ")) + "'"
    print()

    Q1 = 'SELECT nft, owner from nfts WHERE owner={}'.format(wallet)
    cursor.execute(Q1)
    
    print("The name of the NFT and the wallet address of the owner: ")
    print()
    print("+-------------------------------------------------------------------+")
    for nft, owner in cursor:
        nft = str(nft)
        owner = str(owner)
        chars = re.escape(string.punctuation)
        print("| {:<20} | {:<40} |".format(re.sub(r"["+chars+"]", '', nft),(re.sub(r"["+chars+"]", '', owner))))
    print("+-------------------------------------------------------------------+")
    print()
    input("Press anything to return to the menu: ")
    menu()

# ownerToNft(cursor)


def isNftCollectionVerified(cursor):
    nftName = input("Please provide the name of the nft: ")
    print()
    
    Q2 = "SELECT nfts.`nft`, creators.`verified` FROM nfts JOIN creators on nfts.`creator` = creators.walletAddress WHERE `nfts`.nft = '{}'".format(nftName)
    cursor.execute(Q2)

    for verified in cursor:
        if verified[1] == "TRUE":
            print(nftName + " is verified ☑️" )
        elif verified[1] == "FALSE":
            print(nftName + " is not verified ")
    print()
    input("Press anything to return to the menu: ")
    menu()

#isNftCollectionVerified(cursor)

def averageBalanceOfAllUsers(cursor):
    print("Average balance of all the users in the database: ")
    print()

    Q3 = "SELECT AVG(funds) AS averageFunds FROM users"
    cursor.execute(Q3)

    for averageFunds in cursor:
        funds = str(round(averageFunds[0]))
        chars = re.escape(string.punctuation)
        print("+------------+")
        print("|  $ {:<7} |".format(re.sub(r"["+chars+"]", '', funds)))
        print("+------------+")
    print()
    input("Press anything to return to the menu: ")
    menu()



# averageBalanceOfAllUsers(cursor)

def salesOnDate(cursor):
    # Provide a date for all the sales that happened in that day
    # format example:  7/10/2021
    date = "'"+ str(input("Provide a date: ")) + "'"
    print()

    Q4 = "SELECT * FROM sales WHERE date={}".format(date)
    cursor.execute(Q4)
    print("Listing all sales on date " + date + " :")
    print("+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+")
    print("| SaleNumber                     Date                           NftName                         OwnerAddress                                CreatorAddress                              $ Price                       |")
    for saleNumber, date, nftSold, buyer, seller, price in cursor:
        print("| {:<28} | {:<28} | {:<28} | {:<28} | {:<28} | {:<28} |".format(saleNumber, date, nftSold, buyer, seller, price))
    print("+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+")
    print()
    input("Press anything to return to the menu: ")
    menu()

# salesOnDate(cursor)

def menu():
    # This is the Menu where the user chooses different actions and queries that are going to be executed
    print()
    print("This is the Database for NFT Collections, Creators, Users and Sales")
    print()
    print("Input (1) for listing all nfts in the database.")
    print("Input (2) and provide a valid wallet address which will present you with the nft they hold.")
    print("Input (3) and provide the nft name in order to check if the collection is verified.")
    print("Input (4) to be presented with the average amount of funds of all the users in our database.")
    print("Input (5) and provide a valid date(format example:  7/10/2021) to be presented with all the sales that happened on the specific date.")
    print("Input (0) to terminate the program.")
    num = int(input())

    if num == 1:
        listAllNfts(cursor)
    elif num == 2:
        ownerToNft(cursor)
    elif num == 3:
        isNftCollectionVerified(cursor)
    elif num == 4:
        averageBalanceOfAllUsers(cursor)
    elif num == 5:
        salesOnDate(cursor)
    elif num == 0:
        print()
        print("Program Terminated")
        print()
        exit()
    else:
        menu()

menu()

