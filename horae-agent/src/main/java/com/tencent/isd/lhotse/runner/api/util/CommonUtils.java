package com.tencent.isd.lhotse.runner.api.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CommonUtils {

	private CommonUtils() {
	}

	/**
	 * stackTraceToString
	 * 
	 * @param e
	 * @return
	 */
	public static String stackTraceToString(Exception e) {
		if (e == null)
			return null;

		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer, true));
		return writer.toString();
	}

	/**
	 * if at least one string is blank in an string array, return true;<br>
	 * if all string is not blank, return false;
	 * 
	 * @param s
	 * @return
	 */
	public static boolean isBlankAny(String... s) {
		for (int i = 0; i < s.length; i++) {
			if (StringUtils.isBlank(s[i]))
				return true;
		}
		return false;
	}

	// TODO: Add code to fetch output in a thread
	/**
	 * Execute PySql/HiveSql via PLC
	 * 
	 * @param plc
	 * @param user
	 * @param passwd
	 * @param serverip
	 * @param port
	 * @param sql
	 * @param moduleName
	 * @return
	 * @throws IOException
	 */
	// public static String executeSql(String plc, String user, String passwd,
	// String serverip, String port, String sql)
	// throws IOException {
	// StringBuffer output = new StringBuffer();
	//
	// String[] cmds = { plc, user, passwd, serverip, port, sql };
	// log("About to exec sql:");
	// log(Arrays.deepToString(cmds));
	// long startTime = System.currentTimeMillis();
	// BufferedReader outputReader = null, errorReader = null;
	// try {
	// Process process = Runtime.getRuntime().exec(cmds);
	//
	// if (process != null) {
	// output.append("process: ").append(process.toString()).append("\r\n");
	// // read the hive output
	// outputReader = new BufferedReader(new
	// InputStreamReader(process.getInputStream()), 4096);
	// errorReader = new BufferedReader(new
	// InputStreamReader(process.getErrorStream()), 4096);
	// process.waitFor();
	// String line = "";
	// while (outputReader != null && (line = outputReader.readLine()) != null)
	// {
	// output.append(line).append("\r\n");
	// }
	// while (errorReader != null && (line = errorReader.readLine()) != null) {
	// output.append(line).append("\r\n");
	// }
	// long endTime = System.currentTimeMillis();
	// int exitValue = process.exitValue();
	// log("Sql finished in " + (endTime - startTime) + "ms, exit value: " +
	// exitValue);
	//
	// if (exitValue != 0)
	// throw new
	// IOException(String.format("[ERROR] Failed to execute sql, exit value = %d, output=%s",
	// exitValue, output));
	// else
	// log("[SUCCESS] sql was executed.");
	// } else
	// throw new IOException("[ERROR] Failed to create PLC process.");
	// log("Sql output: " + output);
	// } catch (Exception e) {
	// throw new IOException(e);
	// } finally {
	// try {
	// if (outputReader != null)
	// outputReader.close();
	// } finally {
	// if (errorReader != null)
	// errorReader.close();
	// }
	// }
	// return output.toString();
	// }

	/**
	 * merge K-V to StringBuffer
	 * 
	 * @param sb
	 * @param key
	 * @param value
	 */
	public static void mergekeyValue(StringBuffer sb, String key, Object value) {
		sb.append("${").append(key).append("}=").append(value).append("\n");
	}

	/**
	 * save a String to a single text file. note: this may overwrite the corrent
	 * file <br>
	 * To create a new line, try to use: System.getProperty("line.separator"),
	 * not "\n"
	 * 
	 * @param srcStr
	 * @param filePath
	 * @param encoding
	 * @throws IOException
	 */
	public static void string2File(String srcStr, String filePath,
			String encoding) throws IOException {
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			reader = new BufferedReader(new StringReader(srcStr));
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filePath), encoding));
			char buf[] = new char[4096];
			int len;
			while ((len = reader.read(buf)) != -1) {
				writer.write(buf, 0, len);
			}
		} finally {
			try {
				reader.close();
			} finally {
				writer.close();
			}
		}
	}

	/**
	 * getTaskTime
	 * 
	 * @param timeParams
	 * @param currentRunDate
	 * @param offRange
	 * @param cycleUnit
	 * @param day
	 * @return
	 */
	public static String getTaskTime(String timeParams, long currentRunDate,
			String offRange, String cycleUnit, String day) {
		String times = "";
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat();
		if (timeParams.equals("${YYYY}")) {
			sdf = new SimpleDateFormat("yyyy");
			c.setTimeInMillis(currentRunDate);
		} else if (timeParams.equals("${YYYYMM}")) {
			sdf = new SimpleDateFormat("yyyyMM");
			if ("0".equals(offRange)) {
				c.setTimeInMillis(currentRunDate);
				c.add(Calendar.MONTH, 1);
				c.add(Calendar.DAY_OF_MONTH, Integer.parseInt(day) - 1);
			} else if ("1".equals(offRange)) {
				c.setTimeInMillis(currentRunDate);
				c.add(Calendar.DAY_OF_MONTH, Integer.parseInt(day) - 1);
			} else {
				System.out.println("---cycuit=mont;yyyymm ;offrange="
						+ offRange);
			}
		} else if (timeParams.equals("${YYYYMMDD}")) {
			sdf = new SimpleDateFormat("yyyyMMdd");
			// plus dyas
			int off = 0;
			if (cycleUnit.equals("D")) {
				c.setTimeInMillis(currentRunDate);
				off = Integer.parseInt(offRange);
				c.add(Calendar.DAY_OF_MONTH, off);
			} else if (cycleUnit.equals("W")) {
				// this week
				c.setTimeInMillis(currentRunDate);
				off = 7 + (Integer.parseInt(day) - 1)
						+ Integer.parseInt(offRange);
				c.add(Calendar.DAY_OF_WEEK, off);
			} else if (cycleUnit.equals("M")) {
				c.setTimeInMillis(currentRunDate);
				c.add(Calendar.MONTH, 1);
				off = (Integer.parseInt(day) - 1) + Integer.parseInt(offRange);
				c.add(Calendar.DAY_OF_MONTH, off);
			}
		} else if (timeParams.equals("${YYYYMMDDHH}")) {
			sdf = new SimpleDateFormat("yyyyMMddHH");
			int off = 0;
			// plus hours
			if (cycleUnit.equals("H")) {
				off = Integer.parseInt(offRange);
				c.setTimeInMillis(currentRunDate);
				c.add(Calendar.HOUR_OF_DAY, off);
			} else {
				System.out.println("---cycuit!=HOUR;yyyymmDDHH ;offrange="
						+ offRange);
			}

		}
		times = sdf.format(c.getTime());
		return times;
	}

	/**
	 * YYYY,YYYYYMM,YYYYMMDD,YYYYMMDDHH <br>
	 * <br>
	 * 
	 * Test:System.out.println(standardizeParams(
	 * "$ADD(${YYYY},-15),$ADD(${YYYYMM},+5),$ADD(${YYYYMMDD},-10),
	 * $ADD(${YYYYMMDDHH},3),YYYYMMDD,1,'x x','aa',aS" ,
	 * System.currentTimeMillis()));
	 * 
	 * yyyymmdd+offRange yyyymmddhh+offRange
	 * 
	 * @param params
	 * @param currentRunDate
	 * @return
	 */
	public static String standardizeParams(String params, long currentRunDate,
			String offRange, String cycleUnit, String dayNum) throws Exception {
		if (params == null || "".equals(params) || currentRunDate == 0)
			return null;
		StringBuffer standardParam = new StringBuffer();
		//Calendar c = Calendar.getInstance();
		String year = "${YYYY}";
		String month = "${YYYYMM}";
		String day = "${YYYYMMDD}";
		String hour = "${YYYYMMDDHH}";
		try {

			String param[] = params.split(",");

			SimpleDateFormat sdf;
			for (int i = 0; i < param.length; i++) {

				if (param[i].equals(year) || param[i].equals(month)
						|| param[i].equals(day) || param[i].equals(hour)) {
					standardParam.append(getTaskTime(param[i], currentRunDate,
							offRange, cycleUnit, dayNum));
				} else if (param[i].startsWith("$ADD(${YYYYMMDDHH}")) {
					// plus hours
					sdf = new SimpleDateFormat("yyyyMMddHH");
					Calendar c1 = Calendar.getInstance();
					c1.setTime(sdf.parse(getTaskTime(hour, currentRunDate,
							offRange, cycleUnit, dayNum)));

					String fullParam = param[i] + "," + param[++i];
					String offsetStr = fullParam.substring(19,
							fullParam.length() - 1);
					int offset = Integer.parseInt(offsetStr);
					c1.add(Calendar.HOUR_OF_DAY, offset);
					standardParam.append(sdf.format(c1.getTime()));

				} else if (param[i].startsWith("$ADD(${YYYYMMDD}")) {
					// plus days
					sdf = new SimpleDateFormat("yyyyMMdd");
					Calendar c2 = Calendar.getInstance();
					c2.setTime(sdf.parse(getTaskTime(day, currentRunDate,
							offRange, cycleUnit, dayNum)));

					String fullParam = param[i] + "," + param[++i];
					String offsetStr = fullParam.substring(17,
							fullParam.length() - 1);
					int offset = Integer.parseInt(offsetStr);
					c2.add(Calendar.DATE, offset);

					standardParam.append(sdf.format(c2.getTime()));

				} else if (param[i].startsWith("$ADD(${YYYYMM}")) {
					Calendar c3 = Calendar.getInstance();
					sdf = new SimpleDateFormat("yyyyMM");
					c3.setTime(sdf.parse(getTaskTime("${YYYYMM}",
							currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = param[i] + "," + param[++i];
					String offsetStr = fullParam.substring(15,
							fullParam.length() - 1);
					int offset = Integer.parseInt(offsetStr);
					c3.add(Calendar.MONTH, offset);

					standardParam.append(sdf.format(c3.getTime()));

				} else if (param[i].startsWith("$ADD(${YYYY}")) {
					sdf = new SimpleDateFormat("yyyy");
					Calendar c4 = Calendar.getInstance();
					c4.setTime(sdf.parse(getTaskTime("${YYYY}", currentRunDate,
							offRange, cycleUnit, dayNum)));

					String fullParam = param[i] + "," + param[++i];
					String offsetStr = fullParam.substring(13,
							fullParam.length() - 1);
					int offset = Integer.parseInt(offsetStr);
					c4.add(Calendar.YEAR, offset);

					standardParam.append(sdf.format(c4.getTime()));
				} else
					standardParam.append(param[i]);

				if (i != param.length - 1)
					standardParam.append(" ");
			}
		} catch (Exception e) {
			throw new Exception(e);
		}
		return standardParam.toString();
	}

	/**
	 * select * from F_QZ_APPS_ACT_D partition(p_${YYYYMMDD}) t <br>
	 * select * from F_QZ_APPS_ACT_D partition(p_20120717) t <br>
	 * <br>
	 * /data/stage/interface/busid/[YYYYMMDDHH]/ <br>
	 * /data/stage/interface/busid/YYYYMMDDHH/ <br>
	 * 
	 * @param str
	 * @param currentRunDate
	 * @return
	 * @throws Exception
	 */
	public static String standardizeDateString(String str, long currentRunDate)
			throws Exception {
		if (str == null || currentRunDate == 0)
			return null;

		if ("".equals(str))
			return "";

		String rep1 = new SimpleDateFormat("yyyy").format(currentRunDate);
		String rep2 = new SimpleDateFormat("yyyyMM").format(currentRunDate);
		String rep3 = new SimpleDateFormat("yyyyMMdd").format(currentRunDate);
		String rep4 = new SimpleDateFormat("yyyyMMddHH").format(currentRunDate);

		str = str.replaceAll("\\[YYYYMMDDHH\\]", "YYYYMMDDHH");
		str = str.replaceAll("\\[YYYYMMDD\\]", "YYYYMMDD");
		str = str.replaceAll("\\[YYYYMM\\]", "YYYYMM");
		str = str.replaceAll("\\[YYYY\\]", "YYYY");

		str = str.replaceAll("\\$\\{YYYYMMDDHH\\}", rep4);
		str = str.replaceAll("\\$\\{YYYYMMDD\\}", rep3);
		str = str.replaceAll("\\$\\{YYYYMM\\}", rep2);
		str = str.replaceAll("\\$\\{YYYY\\}", rep1);

		return str;
	}

	/**
	 * parse int with default value
	 * 
	 * @param value
	 * @param defaultValue
	 * @return
	 */
	public static int parseInt(String value, int defaultValue) {
		int result = 0;
		try {
			result = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			result = defaultValue;
		}
		return result;
	}

	public static void deleteFile(File deleteFile) {
		if (!deleteFile.exists()) {
			return;
		}

		if (deleteFile.isFile()) {
			deleteFile.delete();
		} else {
			/* Delete all the files in the destination directory first. */
			File[] allDestFiles = deleteFile.listFiles();
			for (File destFile : allDestFiles) {
				destFile.delete();
			}

			deleteFile.delete();
		}
	}

	public static void deleteHdfsFile(FileSystem hdfs, Path deleteFile)
			throws IOException {

		if (!hdfs.exists(deleteFile)) {
			return;
		}

		FileStatus destPathStatus = hdfs.getFileStatus(deleteFile);
		hdfs.delete(deleteFile, destPathStatus.isDir());
	}

	/* Remove the TDW data with the session id, for those redo task. */
	public static boolean removeHDFSFilesWithPattern(String nameNodeUser,
			String nameNodeGroup, String nameNodeIP, int nameNodePort,
			String sessionId) {
		return false;
	}

	/* Create the HDFS temporary path for HDFS, LINUX load in task. */
	public static boolean createTempHDFSPath(String nameNodeUser,
			String nameNodeGroup, String nameNodeIP, int nameNodePort,
			String tmpPath) {
		FileSystem fs = null;
		boolean success = false;
		try {
			Configuration configuration = new Configuration();
			configuration.set("fs.default.name", "hdfs://" + nameNodeIP + ":"
					+ nameNodePort);
			configuration.set("hadoop.job.ugi", nameNodeUser + ","
					+ nameNodeGroup);
			configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
			configuration.setBoolean("fs.file.impl.disable.cache", true);

			fs = FileSystem.get(configuration);

			Path hdfsTempPath = new Path(tmpPath);

			/* Delete the old temporary path first to clean the old data. */
			if (fs.exists(hdfsTempPath)) {
				deleteHdfsFile(fs, hdfsTempPath);
			}

			/* Create the new temporary path. */
			fs.mkdirs(hdfsTempPath);

			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return success;
	}
}
