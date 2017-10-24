#/bin/bash   
echo 1 > /proc/sys/vm/drop_caches
echo 2 > /proc/sys/vm/drop_caches
echo 3 > /proc/sys/vm/drop_caches

make clean
make
for((i=1;i<=10;i++))
do
let "faa=2*$i"
let "cache=32-$faa"
let "look=176+$i*32"
echo "2016 05-01, $look look ahead, 8 forward assembly, 24 cache space of containers, other changes, 200w test, primary_trace_2" >> result.txt
./bin/optsmr adaptive_faa $look 8 24                                
cat optsmr.log >> result.txt
done
