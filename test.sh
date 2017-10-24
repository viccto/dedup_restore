#/bin/bash
echo 1 > /proc/sys/vm/drop_caches
echo 2 > /proc/sys/vm/drop_caches
echo 3 > /proc/sys/vm/drop_caches

make clean
make

echo "2016 05-01, 32 cache, 32 look ahead, 20MB faa, 400w test, primary_trace1" >> result.txt 


./bin/optsmr container
cat optsmr.log >> result.txt
./bin/optsmr chunk
cat optsmr.log >> result.txt
./bin/optsmr adaptive
cat optsmr.log >> result.txt

