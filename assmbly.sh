#/bin/bash   
echo 1 > /proc/sys/vm/drop_caches
echo 2 > /proc/sys/vm/drop_caches
echo 3 > /proc/sys/vm/drop_caches

make clean
make
echo "2016 05-01, 512MB assembly, 4500w test, primary_trace2" >> result.txt

./bin/optsmr assembly
cat optsmr.log >> result.txt
