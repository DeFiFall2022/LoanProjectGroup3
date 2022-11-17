# PACKAGE AREA

using JLD2, LibPQ, Dates, DataFrames, CSV, Statistics

#===============================================#

# CONSTANT AREA - made for readability

const personalPath = "/home/markosch.saure/" # path used to for saving CSV files

const queryAaveData =  # query to get all data from aave specifically
"SELECT 
    e.block_signed_at,
    encode(e.tx_hash, 'hex') AS tx_hash,
    encode(e.topics[1], 'hex') AS identifier,
    encode(e.topics[2], 'hex') AS collateral,
    encode(e.topics[3], 'hex') AS reserve,
    encode(e.topics[4], 'hex') AS user,
    substring(encode(e.data, 'hex') from (1+64*0) for 64) AS purchaseAmount,
    substring(encode(e.data, 'hex') from (1+64*1) for 64) AS liquidatedCollateralAmount,
    substring(encode(e.data, 'hex') from (1+64*2) for 64) AS accruedBorrowInterest,
    substring(encode(e.data, 'hex') from (1+64*3) for 64) AS liquidator,
    substring(encode(e.data, 'hex') from (1+64*4) for 64) AS receiveAToken,
    substring(encode(e.data, 'hex') from (1+64*5) for 64) AS timestamp
FROM public.block_log_events e 
WHERE e.topics @> ARRAY[CAST( '\\x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'AS bytea)] AND
    e.block_signed_at >= '2022-01-01 00:00:00' AND
    e.block_signed_at < '2022-10-01 00:00:00'
"



#===============================================#

# HELPER FUNCTION AREA

# used for testing; this function prints the given number of rows of a df

function printData(df, numRows)
    show(first(df, numRows), allcols=true, allrows=true, truncate=0)
    println("\n")
end

# the addresses of the collateral, reserve, and liquator in AAVE have 39 leading 0's which can be ignored; this function extracts all characters after the 39th char

function short_address(target_text)
    target_text[(end-39):end]
end

#===============================================#

# ACTUAL CODE STARTS HERE

# create connection to the Ethereum db

conn = LibPQ.Connection("dbname=Ethereum user='read_user' password='abc3x756' host=localhost port=5432")

# execute query to extract Aave-specific data, then store on a dataframe

results = execute(conn, queryAaveData)
aaveDF = DataFrame(results)

# Clean up the fields:
# 1) clean up the address data by removing the 39 leading 0's
# 2) clean up the numeric data by parsing as bigInts
# 3) clean up the timestamp data (which is currently in Unix) by converting to an actual date

aaveDF.collateral = short_address.(aaveDF.collateral)
aaveDF.reserve = short_address.(aaveDF.reserve)
aaveDF.user = short_address.(aaveDF.user)
aaveDF.liquidator = short_address.(aaveDF.liquidator)

aaveDF.purchaseamount = parse.(BigInt, aaveDF.purchaseamount, base = 16)
aaveDF.liquidatedcollateralamount = parse.(BigInt, aaveDF.liquidatedcollateralamount, base = 16)
aaveDF.accruedborrowinterest = parse.(BigInt, aaveDF.accruedborrowinterest, base = 16)
aaveDF.receiveatoken = parse.(BigInt, aaveDF.receiveatoken, base = 16)
aaveDF.timestamp = parse.(BigInt, aaveDF.timestamp, base = 16)

transform!(aaveDF, :timestamp => ByRow(x -> Dates.format(unix2datetime(x), "mm-dd-yyyy")) => :timestamp)
#=
#=================================#
# QUERY 1: daily liquidated collateral amount per liquidator
#=================================#

# only look at relevant columns

dailyLiquidatorAmt = aaveDF[:, [:timestamp, :liquidator, :liquidatedcollateralamount]]

# group by date, then by liquidator with respect to the total of all liquidatedcollateralamounts

dailyLiquidatorAmt = combine(groupby(dailyLiquidatorAmt, [:timestamp, :liquidator]), :liquidatedcollateralamount => sum) 

# send to CSV

CSV.write(personalPath * "dailyLiquidatorAmt.csv", dailyLiquidatorAmt)

#printData(dailyLiquidatorAmt, 3)

#=================================#
# QUERY 2: daily liquidated collateral transaction amount per liquidator
#=================================#

# only look at relevant columns

dailyLiquidatorTxAmt = aaveDF[:, [:timestamp, :liquidator, :liquidatedcollateralamount]]

# group by date, then by liquidator with respect to the total transactions (i.e. rows) of all liquidatedcollateralamounts

dailyLiquidatorTxAmt = combine(groupby(dailyLiquidatorTxAmt, [:timestamp, :liquidator]), nrow) 

# send to CSV

CSV.write(personalPath * "dailyLiquidatorTxAmt.csv", dailyLiquidatorTxAmt)

#printData(dailyLiquidatorTxAmt, 3)

#=================================#
# QUERY 3: top liquidators by liquidated collateral amount
#=================================#

# only look at relevant columns

topLiquidatorAmt = aaveDF[:, [:liquidator, :liquidatedcollateralamount]]

# group by date, then by liquidator with respect to the total of all liquidatedcollateralamounts

topLiquidatorAmt = combine(groupby(topLiquidatorAmt, :liquidator), :liquidatedcollateralamount => sum) 

# put in descending order

sort!(topLiquidatorAmt, [:liquidatedcollateralamount_sum], rev = true)

# send to CSV

CSV.write(personalPath * "topLiquidatorAmt.csv", topLiquidatorAmt)

#printData(topLiquidatorAmt, 3)

#=================================#
# QUERY 4: top liquidators wrt liquidated collateral transaction amount 
#=================================#

# only look at relevant columns

topLiquidatorTxAmt = aaveDF[:, [:liquidator, :liquidatedcollateralamount]]

# group by date, then by liquidator with respect to the total transactions (i.e. rows) of all liquidatedcollateralamounts

topLiquidatorTxAmt = combine(groupby(topLiquidatorTxAmt, :liquidator), nrow) 

# put in descending order

sort!(topLiquidatorTxAmt, [:nrow], rev = true)

# send to CSV

CSV.write(personalPath * "topLiquidatorTxAmt.csv", topLiquidatorTxAmt)

#printData(topLiquidatorTxAmt, 3)
=#

#=================================#
# QUERY 5: daily liquidated collateral amount per user
#=================================#

# only look at relevant columns

dailyUserAmt = aaveDF[:, [:timestamp, :user, :liquidatedcollateralamount]]

# group by date, then by user with respect to the total of all liquidatedcollateralamounts

dailyUserAmt = combine(groupby(dailyUserAmt, [:timestamp, :user]), :liquidatedcollateralamount => sum) 

# send to CSV

CSV.write(personalPath * "dailyUserAmt.csv", dailyUserAmt)

#printData(dailyUserAmt, 3)

#=================================#
# QUERY 6: daily liquidated collateral transaction amount per user
#=================================#

# only look at relevant columns

dailyUserTxAmt = aaveDF[:, [:timestamp, :user, :liquidatedcollateralamount]]

# group by date, then by user with respect to the total transactions (i.e. rows) of all liquidatedcollateralamounts

dailyUserTxAmt = combine(groupby(dailyUserTxAmt, [:timestamp, :user]), nrow) 

# send to CSV

CSV.write(personalPath * "dailyUserTxAmt.csv", dailyUserTxAmt)

#printData(dailyUserTxAmt, 3)

#=================================#
# QUERY 7: daily accrued borrow interest and liquidated collateral amount 
#=================================#

# only look at relevant columns

dailyAccruedLiquid = aaveDF[:, [:timestamp, :accruedborrowinterest, :liquidatedcollateralamount]]

# group by date, then by user with respect to the total of all liquidatedcollateralamounts

dailyAccruedLiquid1 = combine(groupby(dailyAccruedLiquid, :timestamp), :liquidatedcollateralamount => sum) 
dailyAccruedLiquid2 = combine(groupby(dailyAccruedLiquid, :timestamp), :accruedborrowinterest => mean) 

# send to CSV

CSV.write(personalPath * "dailyAccruedLiquid_LiquidAmt.csv", dailyAccruedLiquid1)
CSV.write(personalPath * "dailyAccruedLiquid_InterestAmt.csv", dailyAccruedLiquid2)


#printData(dailyUserAmt, 3)

#=================================#
# QUERY 8: daily accrued borrow interest and liquidated collateral transaction amount 
#=================================#

# only look at relevant columns

dailyAccruedLiquidTx = aaveDF[:, [:timestamp, :accruedborrowinterest, :liquidatedcollateralamount]]

# group by date, then by user with respect to the total of all liquidatedcollateralamounts

dailyAccruedLiquidTx1 = combine(groupby(dailyAccruedLiquidTx, :timestamp), nrow) 
dailyAccruedLiquidTx2 = combine(groupby(dailyAccruedLiquidTx, :timestamp), :accruedborrowinterest => sum) 

# send to CSV

CSV.write(personalPath * "dailyAccruedLiquidTx_LiquidTxAmt.csv", dailyAccruedLiquidTx1)
CSV.write(personalPath * "dailyAccruedLiquidTx_InterestAmt.csv", dailyAccruedLiquidTx2)

#printData(dailyUserAmt, 3)
