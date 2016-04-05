package com.tencent.isd.lhotse.runner.api.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegFileNameFilter implements FilenameFilter {
	private final String fileNamePattern;
	
	public RegFileNameFilter(String fileNamePattern) {
		this.fileNamePattern = fileNamePattern.replace(".", "\\.").replace("*", ".*");
	}
	
	@Override
	public boolean accept(File dir, String name) {
		Pattern pattern = Pattern.compile(fileNamePattern);
		Matcher matcher = pattern.matcher(name);
		
		return matcher.matches();
	}
}
