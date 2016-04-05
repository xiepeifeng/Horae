package com.tencent.isd.lhotse.runner.util;

import java.io.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesParser {

	private Properties props;
	private String fileName;

	public PropertiesParser(String fileName) {
		this.fileName = fileName;
		try {
			props = getPropertiesFromClasspath(fileName);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Properties getPropertiesFromClasspath(String propFileName) throws IOException, FileNotFoundException {
		Properties props = new Properties();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null) {
			throw new FileNotFoundException("property file '" + propFileName + "' was not found in the classpath.");
		}
		try {
			props.load(inputStream);
		}
		finally {
			if (inputStream != null)
				inputStream.close();
		}
		return props;
	}

	@SuppressWarnings("unused")
	private Properties getPropertiesFromFile(String propFileName) throws IOException {
		Properties props = new Properties();
		File file = new File(propFileName);
		if (!file.exists()) {
			throw new FileNotFoundException("property file '" + file.getAbsolutePath() + "' not found in the classpath");
		}
		InputStream inputStream = new FileInputStream(file);
		try {
			props.load(inputStream);
		}
		finally {
			if (inputStream != null)
				inputStream.close();
		}
		return props;
	}

	public String getProperty(String key) {
		return props.getProperty(key);
	}

	public Map<String, String> getAllProperties() {
		Map<String, String> map = new HashMap<String, String>();
		Enumeration<?> enu = props.propertyNames();
		while (enu.hasMoreElements()) {
			String key = (String) enu.nextElement();
			String value = props.getProperty(key);
			map.put(key, value);
		}
		return map;
	}

	public void printAll() {
		props.list(System.out);
	}

	public void writeProperties(String key, String value) {
		try {
			OutputStream fos = new FileOutputStream(fileName);
			props.setProperty(key, value);
			props.store(fos, "comments Update key:" + key);
		}
		catch (IOException e) {
		}
	}
}
