package com.cloudera.mayo;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Pig UDF to parse Wikipedia enwiki-latest-pages-meta-current.xml.bz2 data
 * 
 * @author Paul Codding - paul@hortonworks.com
 */
public class ParseRecord extends EvalFunc<Tuple> {


	public ParseRecord() {
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	public Tuple exec(Tuple input) throws IOException {
		String textValue = null;
		String titleValue = null;
		TupleFactory tf = TupleFactory.getInstance();
		String file = (String) input.get(0);
		Tuple t = tf.newTuple();
		//t.append(titleValue);
		t.append(textValue);
		
		return t;
	}
}

