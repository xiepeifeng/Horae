package com.tencent.isd.lhotse.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author: cpwang
 * @date: 2012-4-25
 */
public class LFileUtil {

	private static final DateDirectoryFilter ddf = new DateDirectoryFilter();

	/**
	 * check whether input stream is gzip file or not
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static boolean checkGZipFile(InputStream in) throws IOException {
		boolean b = false;
		int bytesRead = -1;
		byte[] bFirst = new byte[2];
		bytesRead = in.read(bFirst);
		if (bytesRead >= 0) {
			if ((bFirst[0] == (byte) 0x1f) && (bFirst[1] == (byte) 0x8b)) {
				b = true;
			}
		}
		return b;
	}

	public static String getFileDate(String tagName, char startDelimiter, char endDelimiter) {
		int idx1 = tagName.indexOf(startDelimiter, 0);
		if (idx1 <= 0) {
			return null;
		}
		int idx2 = tagName.indexOf(endDelimiter, idx1 + 1);
		if ((idx2 <= 0) || (idx2 < idx1)) {
			return null;
		}
		return tagName.substring(idx1 + 1, idx2);
	}

	public static void removeDir(File dir) {
		if (dir.isDirectory()) {
			File[] fs = dir.listFiles();
			for (File f : fs) {
				removeDir(f);
			}
		}
		dir.delete();
	}

	public static File[] getFiles(File dir, String suffix) {
		FileSuffixFilter fss = new FileSuffixFilter(suffix);
		return dir.listFiles(fss);
	}

	public static File[] getDateDirectories(File dir) {
		return dir.listFiles(ddf);
	}

	private static class FileSuffixFilter implements FileFilter {
		private String suffix;

		public FileSuffixFilter(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File arg0) {
			if (arg0.isFile()) {
				String name = arg0.getName();
				if (name.endsWith(suffix)) {
					return true;
				}
			}
			return false;
		}
	}

	private static class DateDirectoryFilter implements FileFilter {
		@Override
		public boolean accept(File arg0) {
			if (arg0.isDirectory()) {
				String name = arg0.getName();
				if (name.startsWith("2")) {
					return true;
				}
			}
			return false;
		}
	}
}
