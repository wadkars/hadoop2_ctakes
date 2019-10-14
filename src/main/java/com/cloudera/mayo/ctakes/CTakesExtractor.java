package com.cloudera.mayo.ctakes;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ctakes.core.util.DocumentIDAnnotationUtil;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.collection.CollectionProcessingEngine;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.collection.StatusCallbackListener;
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.xml.sax.SAXException;

import com.cloudera.mayo.ConfigUtil;
import com.cloudera.mayo.ctakes.cr.TuplesCollectionReader;

/**
 * Pig UDF to process Wikipedia pages through cTAKES.
 * 
 * @author Paul Codding - paul@hortonworks.com
 * 
 */
public class CTakesExtractor extends EvalFunc<Tuple> {
	public static final String PIPELINE_PATH = "PIPELINE_PATH";
	public static final String CONFIG_PROPERTIES_PATH = "CONFIG_PROPERTIES_PATH";
	public static final String IS_LOCAL="IS_LOCAL";
	private static final int MAX_TIMEOUT_MS =   10 * 60 * 1000; // 1 min
	TupleFactory tf = TupleFactory.getInstance();
	BagFactory bf = BagFactory.getInstance();
	long numTuplesProcessed = 0;
	CpeDescription cpeDesc = null;
	//Properties myProperties = null;
	//String pipelinePath = "";
	/**
	 * Initialize the CpeDescription class.
	 */
	private void initializeFramework() {
		try {
			/*
			System.out.println(this.getClass().getClassLoader()
								.getResourceAsStream("RushPipelineCPE.xml"));
			*/
			//pipelinePath =  "/Users/swadhar/mycode/eclipse-workspace/rush_ctakes/src/main/resources/RushPipelineCPE.xml";
			//path = "/opt/rush/ctakes_config/RushPipelineCPE.xml";
			FileInputStream stream = null;
			if (cpeDesc == null)
				//String path = "/Users/swadhar/mycode/eclipse-workspace/rush_ctakes/src/main/resources/RushPipelineCPE.xml";
				
				cpeDesc = UIMAFramework.getXMLParser().parseCpeDescription(
						new XMLInputSource(this.getClass().getClassLoader()
								.getResourceAsStream("RushPipelineCPE.xml"),
								new File(ConfigUtil.getConfigBasePath())));
				/*				
				stream = new FileInputStream(pipelinePath);
				cpeDesc = UIMAFramework.getXMLParser().parseCpeDescription(
						new XMLInputSource(stream,
								new File(ConfigUtil.getConfigBasePath())));
								*/
		} catch (Exception e) {
			log.warn(e.getMessage());
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		/*
		if(myProperties==null) {
			
			myProperties = UDFContext.getUDFContext().getClientSystemProps();
			if(myProperties==null) {
				myProperties = System.getProperties();
			}
			*.
			/*
			this.pipelinePath = this.myProperties.getProperty(PIPELINE_PATH);
			if(StringUtils.isBlank(this.pipelinePath)) {
				this.pipelinePath = "/opt/rush/ctakes_config/RushPipelineCPE.xml";
			}
			*/
			//String isLocal = (String)this.myProperties.getOrDefault(IS_LOCAL, "false");
			//boolean local = Boolean.parseBoolean(isLocal);
			/*
			String configPath = this.myProperties.getProperty(CONFIG_PROPERTIES_PATH);
			if(StringUtils.isBlank(configPath)) {
				configPath = "/opt/rush/ctakes_config/config.properties";
			}
			*/
			//ConfigUtil.configure(configPath,local);
			//ConfigUtil.configure(local);
		
		/*
		}
		*/
		long started = System.currentTimeMillis();
		Tuple resultOnly = tf.newTuple(2);
		Tuple result = tf.newTuple(5);
		
		String fNameId = input.get(0).toString();
		//Now split it
		int idx = fNameId.lastIndexOf("-");
		String partName = fNameId.substring(idx+1, fNameId.length());
		String fName = fNameId.substring(0,idx);
		result.set(0, fName);
		result.set(1, partName);
		result.set(2, true);
		String inputStr = (String)input.get(1);
		//inputStr = inputStr.replaceAll("\\r|\\n", "");
		//System.out.println(inputStr);
		//result.set(2, ((String)input.get(1)).replace("\n", " ").replace("\r", " "));
		//inputStr="";
		result.set(3, inputStr);
		result.set(4, "");
		try {
			System.out.println(input.get(0) + ": Reported progress");
			progress();
			initializeFramework();
			CollectionProcessingEngine mCPE = UIMAFramework
					.produceCollectionProcessingEngine(cpeDesc);
			// Process the tuples
			System.out.println(input.get(0)
					+ ": Invoked CollectionProcessingEngine");
			DataBag bagToProcess = bf.newDefaultBag();
			bagToProcess.add(input);
			((TuplesCollectionReader) mCPE.getCollectionReader())
					.setBagToProcess(bagToProcess);
			System.out.println(input.get(0)
					+ ": Added Tuples to TuplesCollectionReader");
			mCPE.process();
			StatusCallbackListenerImpl t = new StatusCallbackListenerImpl(
					this.pigLogger);
			mCPE.addStatusCallbackListener(t);
			System.out.println(input.get(0) + ": Registered callback handler");
			while (!t.isComplete()) {
				if (System.currentTimeMillis() - started >= MAX_TIMEOUT_MS) {
					System.err.append(input.get(0) + ": Task timed out");
					t.setComplete(true);
					//result = input;
					//result.append("");
					resultOnly = setResultInTuple2(resultOnly,"TIME_OUT",false);
					t.setResult(resultOnly);
				}
				Thread.sleep(10);
			}
			//result = t.getResult();
			//result.set(0,input.get(1));
			result.set(2, t.getResult().get(0));
			result.set(4, ((String)t.getResult().get(1)).replace("\n", "").replace("\r", ""));
			System.out.println(input.get(0) + ": Completed processing in "
					+ Long.toString(System.currentTimeMillis() - started));
		} catch (ResourceInitializationException e) {
			result.set(2, false);
			result.set(4,ExceptionUtils.getStackTrace(e));
			e.printStackTrace();
			log.error(e.getMessage());
			e.printStackTrace();
		} 
		
		catch (InterruptedException e) {
			result.set(2, false);
			result.set(4,ExceptionUtils.getStackTrace(e));
			e.printStackTrace();
		}
		return result;
	}

	public static Tuple setResultInTuple2(Tuple result,String output, boolean status)   {
		try {
			result.set(1, output);
			result.set(0, status);
		} catch (ExecException e) {
			TupleFactory tf = TupleFactory.getInstance();
			result = tf.newTuple(2);;
			result.append(false);
			result.append(ExceptionUtils.getStackTrace(e));
		}
		
		return result;
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer
	 * .schema.Schema)
	 */
	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			tupleSchema.add(new FieldSchema("fname", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("part", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("parsed", DataType.BOOLEAN));
			tupleSchema.add(new FieldSchema("text", DataType.CHARARRAY));
			//tupleSchema.add(input.getField(0));
			//tupleSchema.add(input.getField(1));
			tupleSchema.add(new FieldSchema("annotations", DataType.CHARARRAY));
			return new Schema(new Schema.FieldSchema(null,tupleSchema, DataType.TUPLE));
			//return tupleSchema;
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Callback listener to handle document lifecycle processing events.
	 * 
	 * @author Paul Codding - paul@hortonworks.com
	 * 
	 */
	class StatusCallbackListenerImpl implements StatusCallbackListener {
		long size = 0;
		PigLogger logger;
		boolean complete = false;
		Tuple result = null;

		public StatusCallbackListenerImpl(PigLogger logger) {
			this.logger = logger;
		}

		/**
		 * Called when the initialization is completed.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#initializationComplete()
		 */
		public void initializationComplete() {
		}

		/**
		 * Called when the batchProcessing is completed.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#batchProcessComplete()
		 * 
		 */
		public void batchProcessComplete() {
			complete = true;
			System.out
					.println("Completed " + numTuplesProcessed + " documents");
		}

		/**
		 * Called when the collection processing is completed.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#collectionProcessComplete()
		 */
		public void collectionProcessComplete() {
			complete = true;
			System.out
					.println("Completed " + numTuplesProcessed + " documents");
		}

		/**
		 * Called when the CPM is paused.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#paused()
		 */
		public void paused() {
			System.out.println("Paused");
		}

		/**
		 * Called when the CPM is resumed after a pause.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#resumed()
		 */
		public void resumed() {
			System.out.println("Resumed");
		}

		/**
		 * Called when the CPM is stopped abruptly due to errors.
		 * 
		 * @see org.apache.uima.collection.processing.StatusCallbackListener#aborted()
		 */
		public void aborted() {
			complete = true;
			System.err.println("Aborted");
		}

		/**
		 * Called when the processing of a Document is completed. <br>
		 * The process status can be looked at and corresponding actions taken.
		 * 
		 * @param aCas
		 *            CAS corresponding to the completed processing
		 * @param aStatus
		 *            EntityProcessStatus that holds the status of all the
		 *            events for aEntity
		 */
		public void entityProcessComplete(CAS aCas, EntityProcessStatus aStatus) {
			result = tf.newTuple(2);;
			if (aStatus.isException()) {
				List<?> exceptions = aStatus.getExceptions();
				//Tuple t = tf.newTuple(2);
				String errors = "";
				for (int i = 0; i < exceptions.size(); i++) {
					((Throwable) exceptions.get(i)).printStackTrace();
					Throwable ex = ((Throwable) exceptions.get(i));
					errors = errors + ex.getMessage();
					//result = t;
				}
				result = setResultInTuple2(result,errors,false);


				//result = t;
				return;
			} else {
				//Tuple t = tf.newTuple();
				String documentId;
				try {
					JCas jCas = aCas.getJCas();
					documentId = DocumentIDAnnotationUtil.getDocumentID(jCas);
					ByteArrayOutputStream out = new ByteArrayOutputStream();
					XCASSerializer.serialize(aCas, out, true);
					String annotations = out.toString();
					String documentText = aCas.getDocumentText();
					// Strip newlines
					documentText = documentText.replace("\n", " ");
					annotations = annotations.replace("\n", " ");
					//t.append(documentId);
					//t.append(documentText);
					result = setResultInTuple2(result,annotations,true);
					//t.append(annotations);
					//result = t;
				} catch (CASException e) {
					//t.append("CASException " + e.getMessage());
					//result = t;
					result = setResultInTuple2(result,ExceptionUtils.getStackTrace(e),false);
					//e.printStackTrace();
				} catch (SAXException e) {
					result = setResultInTuple2(result,ExceptionUtils.getStackTrace(e),false);
					e.printStackTrace();
				} catch (IOException e) {
					result = setResultInTuple2(result,ExceptionUtils.getStackTrace(e),false);
				}
			}

			numTuplesProcessed++;
		}

		/**
		 * Is the document processing complete?
		 * 
		 * @return
		 */
		public boolean isComplete() {
			return complete;
		}

		/**
		 * Set this handler to be complete.
		 * 
		 * @param complete
		 */
		public void setComplete(boolean complete) {
			this.complete = complete;
		}

		/**
		 * Get the tuple containing the annotations.
		 * 
		 * @return
		 */
		public Tuple getResult() {
			return result;
		}

		public void setResult(Tuple result) {
			this.result = result;
		}
	}
	
	public static void main(String[] args) throws Exception {
		CTakesExtractor p = new CTakesExtractor();
		TupleFactory tf = TupleFactory.getInstance();
		List<String> l = new ArrayList<>();
		l.add("/tmp/CTAKES_DATA/9380.txt-1");
		l.add("Nasal trauma is an injury to your nose or the areas that surround and support your nose. Internal or external injuries can cause nasal trauma. The position of your nose makes your nasal bones, cartilage, and soft tissue particularly vulnerable to external injuries");
		
		//String s = FileUtils.readFileToString(new File("/tmp/CTAKES_DATA/9380.txt"));
		//System.out.println(s);
		//l.add(s);
		Tuple t = tf.newTuple(l);
		Tuple o = p.exec(t);
		System.out.println(o.get(0) + "\n" + o.get(1) + "\n" + o.get(2) + "\n" +o.get(3));
		System.err.println(o.get(0));
		System.err.println(o.get(1));
		System.err.println(o.get(2));
		System.err.println(o.get(3));
		System.err.println(o.get(4));
		//System.out.println(o.get(2));
		//System.out.println(o.get(3));
		//System.err.println(o.get(1));
		//System.err.println(o.get(2));
		//System.out.println(o.size());
	}
}