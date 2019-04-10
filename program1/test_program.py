import sys
sys.path.append("/opt/Utils/Modules/")
import configReader
import os
import pytest
import datetime
import subprocess

input_data_path =  "/path/to/test/data/test_data/"

#Create a dict of sections in the configuration file
def get_config(sectionName):
    config = configReader.configReader()
    config_path = "/path/to/config/file/config.ini"
    srcVal = config.getSectionDict(config_path,sectionName)
    return srcVal

@pytest.fixture(scope="session")
def execute_script():
    os.system("spark-submit --master yarn-cluster --conf spark.io.compression.codec=snappy --conf spark.scheduler.listenerbus.eventqueue.size=30000 --conf spark.yarn.queue=Aggregation --conf spark.driver.cores=5 --conf spark.dynamicAllocation.minExecutors=100 --conf spark.dynamicAllocation.maxExecutors=300 --conf spark.shuffle.compress=true --conf spark.sql.tungsten.enabled=true --conf spark.shuffle.spill=true  --conf spark.sql.parquet.compression.codec=snappy --conf spark.speculation=true --conf spark.kryo.referenceTracking=false --conf spark.hadoop.parquet.block.size=134217728 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.executor.memory=22g --conf spark.hadoop.dfs.blocksize=134217728 --conf spark.shuffle.manager=sort --conf spark.driver.memory=25g --conf spark.hadoop.mapreduce.input.fileinputformat.split.minsize=134217728 --conf spark.akka.frameSize=1024 --conf spark.yarn.executor.memoryOverhead=3120 --conf spark.sql.parquet.filterPushdown=true --conf spark.sql.inMemoryColumnarStorage.compressed=true --conf spark.hadoop.parquet.enable.summary-metadata=false --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.rdd.compress=true --conf spark.task.maxFailures=50 --conf spark.yarn.max.executor.failures=30 --conf spark.yarn.maxAppAttempts=1  --conf spark.default.parallelism=2001 --conf spark.network.timeout=1200s --conf spark.hadoop.dfs.client.read.shortcircuit=true --conf spark.dynamicAllocation.enabled=true --conf spark.executor.cores=5 --conf spark.yarn.driver.memoryOverhead=5024 --conf spark.shuffle.consolidateFiles=true --conf spark.sql.parquet.mergeSchema=false --conf spark.sql.avro.compression.codec=snappy --conf spark.hadoop.dfs.domain.socket.path=/var/lib/hadoop-hdfs/dn_socket --conf spark.shuffle.spill.compress=true --conf spark.sql.caseSensitive=true --conf spark.hadoop.mapreduce.use.directfileoutputcommitter=true --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 --conf spark.sql.shuffle.partitions=2001 --packages com.databricks:spark-csv_2.10:1.3.0,com.databricks:spark-avro_2.10:2.0.1 --class abc.xyz /path/to/jar/file/sample.jar 0.001 1")

def create_input_data_folder():
    now = datetime.datetime.now()
    weekday = now.weekday()
    get_year = r"""date -d "0 day ago" +%Y"""
    p1 = subprocess.Popen(get_year,stdout=subprocess.PIPE,shell=True)
    year = p1.stdout.read().strip()
    get_month = r"""date -d "0 day ago" +%m"""
    p2 = subprocess.Popen(get_month,stdout=subprocess.PIPE,shell=True)
    month = p2.stdout.read().strip()

    #In java, monday = "2" and in python for monday = "0"
    if weekday == 0:
        get_day_1 = r"""date -d "2 day ago" +%d"""
        p3 = subprocess.Popen(get_day_1,stdout=subprocess.PIPE,shell=True)
        day_1 = p3.stdout.read().strip()
        get_day_2 = r"""date -d "3 day ago" +%d"""
        p4 = subprocess.Popen(get_day_2,stdout=subprocess.PIPE,shell=True)
        day_2 = p4.stdout.read().strip()
        input_path1 = "/ml/agg_recon_dailyAgg/"+year+"_"+month+"_"+day_1
        input_path2 = "/ml/agg_recon_dailyAgg/"+year+"_"+month+"_"+day_2
        os.system("hadoop fs -rm -r "+ input_path1)
        os.system("hadoop fs -rm -r "+ input_path2)
        os.system("hadoop fs -mkdir -p "+ input_path1)
        os.system("hadoop fs -mkdir -p "+ input_path2)
    else:
        get_day_1 = r"""date -d "1 day ago" +%d"""
        p3 = subprocess.Popen(get_day_1,stdout=subprocess.PIPE,shell=True)
        day_1 = p3.stdout.read().strip()
        get_day_2 = r"""date -d "2 day ago" +%d"""
        p4 = subprocess.Popen(get_day_2,stdout=subprocess.PIPE,shell=True)
        day_2 = p4.stdout.read().strip()
        input_path1 = "/ml/agg_recon_dailyAgg/"+year+"_"+month+"_"+day_1
        input_path2 = "/ml/agg_recon_dailyAgg/"+year+"_"+month+"_"+day_2
        os.system("hadoop fs -rm -r "+ input_path1)
        os.system("hadoop fs -rm -r "+ input_path2)
        os.system("hadoop fs -mkdir -p "+ input_path1)
        os.system("hadoop fs -mkdir -p "+ input_path2)
    return input_path1,input_path2

def create_output_path():
    get_year = r"""date -d "0 day ago" +%Y"""
    p1 = subprocess.Popen(get_year,stdout=subprocess.PIPE,shell=True)
    year = p1.stdout.read().strip()
    get_month = r"""date -d "0 day ago" +%m"""
    p2 = subprocess.Popen(get_month,stdout=subprocess.PIPE,shell=True)
    month = p2.stdout.read().strip()
    get_day = r"""date -d "0 day ago" +%d"""
    p3 = subprocess.Popen(get_day,stdout=subprocess.PIPE,shell=True)
    day = p3.stdout.read().strip()
    current_date = year+"-"+month+"-"+day
    output_path = "/ml/throttle_tld_modeldata/"+current_date
    return output_path

@pytest.fixture(scope="function")
def put_data_to_hdfs():
    get_input = create_input_data_folder()
    os.system("hadoop fs -put "+input_data_path+"/part-00000 "+get_input[0]+"/")
    os.system("hadoop fs -put "+input_data_path+"/part-00001 "+get_input[1]+"/")
    execute_script()

#1
def test_check_domain_ask_fm():
    put_data_to_hdfs()
    output_path = create_output_path()
    src_val = get_config("test_check_domain_ask_fm")
    domain_name = src_val["domain_name"]
    expected_result = src_val["expected_result"]
    awk_command = "hadoop fs -cat "+output_path+"/part* | awk -F ',' '{if ($3=="+'"'+domain_name+'"'+" && $4==0) {print $5,$6,$7}}'"
    output = subprocess.Popen(awk_command, stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    assert expected_result == output

#2
def test_domain_mytest_com_should_not_be_in_result():
    src_val = get_config("test_domain_mytest_com_should_not_be_in_result")
    domain_name = src_val["domain_name"]
    expected_result = src_val["expected_result"]
    output_path = create_output_path()
    awk_command = "hadoop fs -cat "+output_path+"/part* | awk -F ',' '{if ($3=="+'"'+domain_name+'"'+" && $4==0) {print $5,$6,$7}}'"
    output = subprocess.Popen(awk_command, stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    assert expected_result == output

#3
def test_check_domain_integral_calculator_com():
    src_val = get_config("test_check_domain_integral_calculator_com")
    domain_name = src_val["domain_name"]
    expected_result = src_val["expected_result"]
    output_path = create_output_path()
    awk_command = "hadoop fs -cat "+output_path+"/part* | awk -F ',' '{if ($3=="+'"'+domain_name+'"'+" && $4==0) {print $5,$6,$7}}'"
    output = subprocess.Popen(awk_command, stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    assert expected_result == output


#4
def test_abc_xyz_com_should_not_be_in_result():
    src_val = get_config("test_sixguns_wikia_com_should_not_be_in_result")
    domain_name = src_val["domain_name"]
    expected_result = src_val["expected_result"]
    output_path = create_output_path()
    awk_command = "hadoop fs -cat "+output_path+"/part* | awk -F ',' '/"+domain_name+"/ {print 0}'"
    output = subprocess.Popen(awk_command, stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    assert expected_result == output

#5
def test_pqr_com_should_not_be_in_result():
    src_val = get_config("test_destructoid_com_should_not_be_in_result")
    domain_name = src_val["domain_name"]
    expected_result = src_val["expected_result"]
    output_path = create_output_path()
    awk_command = "hadoop fs -cat "+output_path+"/part* | awk -F ',' '/"+domain_name+"/ {print 0}'"
    output = subprocess.Popen(awk_command, stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    assert expected_result == output
