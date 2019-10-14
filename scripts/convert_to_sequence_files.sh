echo 'RAW_FILES_PATH=' $1
echo 'SEQUENCE_FILE_PATH=' $2
echo 'NO_OF_REDUCERS=' $3
echo 'MAX_FILE_SPLIT_SIZE' = $4
hadoop jar hadoop2_ctakes-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.cloudera.mayo.SmallFilesToSequenceFile $1 $2 $3 $4 