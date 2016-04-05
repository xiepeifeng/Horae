package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDFSRegExprFilter implements PathFilter {
	private final String fileName;
	private final String fileNamePattern;
	private final AbstractTaskRunner runner;
	
	public HDFSRegExprFilter(String fileName, AbstractTaskRunner runner) {
		this.fileName = fileName;
		this.fileNamePattern = 
			fileName.replace(".", "\\.").replace("*", ".*");
		this.runner = runner;
	}
	
	@Override
	public boolean accept(Path path) {
		Pattern pattern = Pattern.compile(fileNamePattern);
		Matcher matcher = pattern.matcher(path.getName());
		
		boolean matched = matcher.matches();
		if (matched) {
		    runner.writeLocalLog(Level.INFO, 
		    		             "Want to find file: " + fileName +
		    		             ", the actual file: " + path.getName() + 
		    		             " does match");
		} else {
			runner.writeLocalLog(Level.INFO, 
		                         "Want to find file: " + fileName +
		                         ", the actual file: " + path.getName() + 
		                         " doesn't match");
		}
		
		return matched;
	}
}
