package com.tencent.isd.lhotse.runner.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class CopyLogFileFilter implements PathFilter {
	private final String fileNamePattern;
	
	public CopyLogFileFilter(String fileNamePattern) {
		this.fileNamePattern = fileNamePattern;
	}
	
	@Override
	public boolean accept(Path path) {
		if (path.getName().startsWith(fileNamePattern)) {
			return true;
		}
		
		return false;
	}
}
