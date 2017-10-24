#/bin/bash   
echo 1 > /proc/sys/vm/drop_caches
echo 2 > /proc/sys/vm/drop_caches
echo 3 > /proc/sys/vm/drop_caches

make clean
make
for((j=1;j<=20;j++))
do
for((i=1;i<=15;i++))
do
let "faa=4*$i"
let "cache=64-$faa"
let "look=$j*64"
echo "$look $faa $cache " >> result1.txt
./bin/optsmr faa_cache $look $faa $cache                                
cat optsmr.log >> result.txt
done
done
