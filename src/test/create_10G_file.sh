
ll -h ../../data/raw
for i in {1..20}; do
    cat ../../data/raw/D1_0.csv >> ../../data/raw/D1_10G.csv
done


for i in {1..7}; do 
    cat ../../data/raw/D1_0.csv >> ../../data/raw/D1_10G.csv
done

cd /Users/jupiter/Downloads/tianchi.eleme.data;
for i in {2..9}; do 
    unzip D1_${i}.csv.zip && rm -rf D1_${i}.csv.zip && gzip D1_${i}.csv
done


unzip -p file.zip | tar -czf file.tar.gz -T -
