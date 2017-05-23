#!/bin/bash
##
## Helper script to do the following:
##	1. Copies .zip files from mounted directory (~/data in this case)
##	2. Extracts .csv file from each .zip
##	3. Extracts relevant (filtered) set of columns (handles commas in quotes, etc)
##	4. The extracted file is renamed
##	5. The extracted file is then copied over to hdfs
##	6. All the files are deleted from local dir
##	7. A blank file with _DONE suffix is created locally
##
for f in `find /home/ubuntu/data/aviation/airline_ontime/$1 -name "*.zip"`
do
        echo --Processing $f--
        unzip -o $f
        y=`basename $f`
        z=${y%.zip}.csv
        nf=${z%.csv}.filtered
        awk -vFPAT='[^,]*|"[^"]*"' '{print  $1","$2","$3","$4","$5","$6","$11","$12","$18","$24","$25","$26","$27","$30","$35","$36","$37","$38","$41","$42","$47","$45","$46","$49}' $z > $nf
        rm -f $z
        mv $nf $nf.csv
        hdfs dfs -mkdir -p airline/stage/$1
        hdfs dfs -copyFromLocal $nf.csv airline/stage/$1
        mv $nf.csv $nf.csv._DONE
        echo ---- done with $f ----
        #exit 0
done;
rm -f *.html