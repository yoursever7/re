result_file := ../../result/spark_result
data_dir := ../../alsdata/
result_log_dir := ../../log/
post_eval_result := ../../result/post_eval_result
model_arg_file := ../../config/alsmodel.config
rec_list_file := ../../alsdata/global_cf_result.data
num := 10
parm_file := ./parm
#rec_list_file := ../logs/100_10_0.03_10_log/part-00000

data:
	python ../dev_160827/generateFeatures.py > log/data.log 
als_many:
	./als_many.sh    
sel_als:
	python sel_als.py  > log/sel_als.log
train:
	$(SPARK_HOME)/bin/spark-submit --master spark://10.105.247.189:7077 ../dev_160827/trainModel.py  
reco:
	$(SPARK_HOME)/bin/spark-submit --master spark://11.105.247.189:7077 ../dev_160827/cfRecommendation.py '0000-00-00 00:00:00' > recommend.log 
# computing recall anc precision based on combined recommendation
post_eval:
	$(SPARK_HOME)/bin/spark-submit --master spark://10.105.247.189:7077 post_eval.py > log/post_eval.log
# to insert ml-20m data into mysql  clothing and usr table
sql_in_usr:
	$(SPARK_HOME)/bin/spark-submit --master spark://10.105.247.189:7077 sql_v4.py > sql_v4.log 
# to insert ml-20m data into mysql-try and collect table
sql_in:
	$(SPARK_HOME)/bin/spark-submit --master spark://10.105.247.189:7077 sql_v3.py > sql_v3.log 
# to insert ml-20m data into mysql-try table
sql:
	$(SPARK_HOME)/bin/spark-submit --master spark://10.105.247.189:7077 sql_v2.py > sql.log
sql_query:
	python sql_query.py > sql_query.log
# ######################################################################
# non available
eval:
	python graph.py  $(result_file)	$(parm_file)	
dist:
	python rec_list.py $(rec_list_file) $(num)
clean:
	rm ../alsmodel/* -rf 
