package com.cloudera.mayo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * @author Paul Codding - paul@hortonworks.com
 *
 */
public class ConfigUtil {
	static Properties properties = new Properties();
	static boolean local = false;
	static {
		
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		InputStream stream = loader.getResourceAsStream("config.properties");
		

		
		try {
			
			properties.load(stream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	/*
	public static void configure(String propertiesFilePath,boolean local) {
		try {
			ConfigUtil.local = local;
			InputStream stream = new FileInputStream(propertiesFilePath);
			properties.load(stream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void configure(boolean local) {

			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			InputStream stream = loader.getResourceAsStream("config.properties");
			ConfigUtil.local = local;
			try {
				
				properties.load(stream);
			} catch (IOException e) {
				e.printStackTrace();
			}

	}
	*/
	public static String getConfigBasePath() {
		return properties.getProperty("configBasePath");
		/*
		if(local) {
			return properties.getProperty("configBasePathLocal");
		}else {
			return properties.getProperty("configBasePath");
		}
		*/
		
	}

	public static String getPropertyByName(String name) {
		return properties.getProperty(name);
	}
	
}
