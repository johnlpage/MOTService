#!/bin/bash

# This downloads the data and loads it into MySQL
# IT doesnt start up MySQL for you

MYSQLCMD="/usr/local/mysql/bin/mysql  --local-infile -uroot -p2efdaf4b59 mot "
MDB="mottest"



mkdir rawdata
cd rawdata

if [ ! -f  dft_test_result_2021.zip ]
then
    curl -OL https://data.dft.gov.uk/anonymised-mot-test/test_data/dft_test_result_2021.zip
fi

if [ ! -f  dft_test_item_2021.zip ]
then
    curl -OL https://data.dft.gov.uk/anonymised-mot-test/test_data/dft_test_item_2021.zip
fi

if [ ! -f  lookup.zip ]
then
    curl -OL https://data.dft.gov.uk/anonymised-mot-test/lookup.zip
fi




unzip -n lookup.zip

#Files are called 2022 but data is 2021
#Combine and sort data, faster to load as explits clustered primary keys
echo "Combining files - this can take a very long time"

if [ ! -f test_result_2021.csv ]
then

    unzip -n dft_test_result_2021.zip
    cat test_result_2022/test_result*.csv | sort -n | grep -v '^"test_id"' > test_result_2021.csv
    rm test_result_2022/test_result*.csv
fi

if [ ! -f test_item_2021.csv ]
then
    unzip -n dft_test_item_2021.zip
    cat test_item_2021/test_item*.csv | sort -n | grep -v '^"test_id"' > test_item_2021.csv
    rm test_item_2021/test_item*.csv 
fi

echo "Files combined."
$MYSQLCMD < ../createmot.sql

#Load the SQL


#Build the test tool

