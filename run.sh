#! /usr/bin/sh

date

if [[ $1 == "fresh" ]] || [[ $1 == "FRESH" ]] || [[ $1 == "Fresh" ]];
then
    python create_tbl_new.py
    echo "Starting scraper!!!"
    python malscraper.py
    echo "Finished!!!"
elif [[ $1 == "refresh" ]] || [[ $1 == "REFRESH" ]] || [[ $1 == "Refresh" ]];
then
    python create_tbl.py
    echo "Starting scraper!!!"
    python malscraper.py
    echo "Finished!!!"
else
    # python3 create_tbl.py
    echo "Starting scraper!!!"
    python malscraper.py
    echo "Finished!!!"
fi