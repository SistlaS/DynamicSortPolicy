#!/bin/bash

echo "=============================="
echo " STARTING SORT JOIN"
echo "=============================="
time spark-submit --master spark://master:7077 /src/DynamicSortPolicy/scripts/sort_join.py
echo "✔ DONE SORT JOIN"
echo ""

echo "=============================="
echo " STARTING OUR ALGORITHM"
echo "=============================="
time spark-submit --master spark://master:7077 /src/DynamicSortPolicy/scripts/sort_all_in_one.py
echo "✔ DONE OUR ALGO"
echo ""

echo "=============================="
echo " STARTING AQE JOIN"
echo "=============================="
time spark-submit --master spark://master:7077 /src/DynamicSortPolicy/scripts/aqe.py
echo "✔ DONE AQE JOIN"
echo ""