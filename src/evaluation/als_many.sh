#!/bin/bash

#result file
# map etc.
#format: "rank	iteration	lambda	alpha	topK	MSE	MAP	run_time" 
result_file="/data/recommend/offline/result/spark_result"
parm_file="/data/recommend/offline/src/evaluate/parm"

if [ ! -f "$result_file" ]
then
    touch "$result_file"
else
    postfix=`date +%Y%m%d-%H%M%S`
    new_log="$result_file""$postfix"
    mv "$result_file" "$new_log"
    touch "$result_file"
fi


while read line
do
    rank_l=${iteration_l}
    iteration_l=${lambda_l}
    lambda_l=${alpha_l}
    alpha_l=${num_l}
    num_l=${line}
done < ${parm_file}


for k in ${num_l}  
do
    for rank in ${rank_l} #50 80 100 120 150 200
    do
        for iteration in ${iteration_l} #5 10 15 20 
        do
            for lambda in   ${lambda_l} #0.001 #0.005 0.007  0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1 0.12 0.14 0.16
            do
                for alpha in ${alpha_l} #0.01 #0.1 
                do
                    $SPARK_HOME/bin/spark-submit --master spark://10.105.247.189:7077 als_recommend.py $rank $iteration $lambda $alpha $k >> log/als_many.log 
                done
            done
        done
    done
done
