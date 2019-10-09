set mapreduce.map.memory.mb    2000
set mapreduce.reduce.memory.mb 2000
set default_parallel 10;
register /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar;
register hadoop2_ctakes-0.0.1-SNAPSHOT-jar-with-dependencies.jar;
register jcarafe-core_2.9.1-0.9.8.3.RC4.jar;
register jcarafe-ext_2.9.1-0.9.8.3.RC4.jar;
register med-facts-i2b2-1.2-SNAPSHOT.jar;
register med-facts-zoner-1.1.jar;
DEFINE PROCESSPAGE com.cloudera.mayo.ctakes.CTakesExtractor();

finalTable = LOAD '$DUMMY_HIVE_TBL_NAME' USING org.apache.hive.hcatalog.pig.HCatLoader();


A = LOAD '$DOCS_INPUT_PATH' USING  org.apache.pig.piggybank.storage.SequenceFileLoader() AS (key:chararray,value:chararray);;
C = FOREACH A GENERATE FLATTEN(PROCESSPAGE($0, $1));
D = UNION finalTable,C;
store D into '$HIVE_TBL_NAME' using org.apache.hive.hcatalog.pig.HCatStorer('loaded=$RUNDT','title: chararray,parsed: boolean,text: chararray,annotations: chararray');