# PACKAGE AREA

using JLD2, LibPQ, Dates, DataFrames, CSV

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

#=
#=================================#
# QUERY 5: top miners wrt liquidated collateral amount
#=================================#

# only look at relevant columns

topMinerAmt = aaveDF[:, [:miner, :liquidatedcollateralamount]]

# group by date, then by miner with respect to the total transactions (i.e. rows) of all liquidatedcollateralamounts

topMinerAmt = combine(groupby(topMinerAmt, :miner), :liquidatedcollateralamount => sum) 

# put in descending order

sort!(topMinerAmt, [:liquidatedcollateralamount_sum], rev = true)

# send to CSV

#CSV.write(personalPath * topMinerAmt * ".csv", topMinerAmt)

printData(topMinerTxAmt, 3)

#=================================#
# QUERY 6: liquidated collateral amount amount between miners and liquidators
#=================================#

# only look at relevant columns

topMinerLiquidatorAmt = aaveDF[:, [:liquidator, :miner, :liquidatedcollateralamount]]

# group by liquidator, then by miner with respect to the number of transactions (i.e. rows) of all liquidatedcollateralamounts

topMinerLiquidatorAmt = combine(groupby(topMinerLiquidatorAmt, [:liquidator, :miner]), :liquidatedcollateralamount => sum) 

# put in descending order

sort!(topMinerLiquidatorAmt[:liquidatedcollateralamount_sum], rev = true)

# send to CSV

#CSV.write(personalPath * topMinerLiquidatorAmt * ".csv", topMinerLiquidatorAmt)

printData(topMinerLiquidatorAmt, 3)
=#

