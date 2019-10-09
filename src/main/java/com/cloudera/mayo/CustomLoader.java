package com.cloudera.mayo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomLoader extends LoadFunc {
	private static final Logger LOGGER = LoggerFactory.getLogger(CustomLoader.class);
	private RecordReader<LongWritable, Text> reader = null;
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private static final int COLUMNS_COUNT = 5;
	private static final String DELIMETER = ",";

	public CustomLoader() {
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
		return new MyTextInputFormat();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		List<String> resultList = null;
		int resultLength = 0;
		String[] tupleArray = null;
		boolean validRecord = false;
		Tuple tuple = tupleFactory.newTuple(5);
		try {
			if (!reader.nextKeyValue()) {
				return null;
			}
			resultLength = COLUMNS_COUNT;
			resultList = new ArrayList<String>(1);
			for (int i = 0; i < resultLength; i++) {
				resultList.add("");
			}
			//resultList.set(0, "INVALID");
			Text value = (Text) reader.getCurrentValue();
			resultList.set(0, new String(value.getBytes()));
			/*
			if (value != null && value.getLength() > 0) {
				tupleArray = value.toString().split(DELIMETER, -1);
				if (tupleArray.length == COLUMNS_COUNT && !value.toString().contains(ServiceID)) {
					validRecord = true;
					for (int i = 0; i < COLUMNS_COUNT; i++) {
						if (i == 1) {
							tupleArray[1] = tupleArray[i].split(tupleArray[0] + ".")[1];
						}
						resultList.set(i + 1, (tupleArray[i]));
						if (tupleArray[i].contains("\u0000")) {
							validRecord = false;
						}
					}
					if (validRecord) {
						resultList.set(0, "VALID");
					}
				}
			}
			*/
			tuple = this.tupleFactory.newTupleNoCopy(resultList);
		} catch (InterruptedException e) {
			LOGGER.info(e.getLocalizedMessage());
		}
		return tuple;
	}
}