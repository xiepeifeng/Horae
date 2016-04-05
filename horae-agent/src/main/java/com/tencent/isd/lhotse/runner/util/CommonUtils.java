package com.tencent.isd.lhotse.runner.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		String valueStr = convertToXMLFormat(String.valueOf(value));
		sb.append("${").append(key).append("}=").append(valueStr).append("\n");
	}

	/**
	 * convert string to xml format
	 * 
	 * @param s
	 * @return
	 */
	public static String convertToXMLFormat(String s) {
		if (s == null)
			return null;
		s = StringUtils.replace(s, "<", "&lt;");
		s = StringUtils.replace(s, ">", "&gt;");
		s = StringUtils.replace(s, "&", "&amp;");
		s = StringUtils.replace(s, "'", "&apos;");
		s = StringUtils.replace(s, "\"", "&quot;");
		return s;
	}

	/**
	 * save a String to a single text file. note: this may overwrite the corrent file <br>
	 * To create a new line, try to use: System.getProperty("line.separator"), not "\n"
	 * 
	 * @param srcStr
	 * @param filePath
	 * @param encoding
	 * @throws IOException
	 */
	public static void string2File(String srcStr, String filePath, String encoding) throws IOException {
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			reader = new BufferedReader(new StringReader(srcStr));
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), encoding));
			char buf[] = new char[4096];
			int len;
			while ((len = reader.read(buf)) != -1) {
				writer.write(buf, 0, len);
			}
		}
		finally {
			try {
				reader.close();
			}
			finally {
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
	public static String getTaskTime(String timeParams, long currentRunDate, String offRange, String cycleUnit,
			String day) {
		String times = "";
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(currentRunDate);
		SimpleDateFormat sdf = new SimpleDateFormat();
		if (timeParams.equals("${YYYY}")) {
			sdf = new SimpleDateFormat("yyyy");
			if (offRange != null) {
//				if ("0".equals(offRange)) {
//					c.add(Calendar.YEAR, 1);
//				}
//				else {
//					int off = -Integer.parseInt(offRange);
//					c.add(Calendar.YEAR, off);
//				}
				System.out.println("strange:cycuit="+cycleUnit+" param=yyyy ;offrange=" + offRange);
			}
		}
		else if (timeParams.equals("${YYYYMM}")) {
			sdf = new SimpleDateFormat("yyyyMM");
			if (offRange != null) {
				if (cycleUnit.equals("D")) {
					c.add(Calendar.DAY_OF_MONTH, 1);
					if (!"0".equals(offRange)) {
						int off = -Integer.parseInt(offRange);
						c.add(Calendar.MONTH, off);
					}
				}
				else if (cycleUnit.equals("M")) {
					c.add(Calendar.MONTH, 1);
					if ("0".equals(offRange)) {
						c.add(Calendar.DAY_OF_MONTH, Integer.parseInt(day) - 1);
					}
					else if ("1".equals(offRange)) {
						int off = -Integer.parseInt("1");
						c.add(Calendar.MONTH, off);
						c.add(Calendar.DAY_OF_MONTH, Integer.parseInt(day) - 1);
					}
					else {
						int off = -Integer.parseInt(offRange);
						c.add(Calendar.MONTH, off);
						c.add(Calendar.DAY_OF_MONTH, Integer.parseInt(day) - 1);
						System.out.println("---cycuit=mont;yyyymm ;offrange=" + offRange);
					}
				}
				else {
					System.out.println("---cycuit is not month or Day; yyyymm ;offrange=" + offRange);
				}
			}
		}
		else if (timeParams.equals("${YYYYMMDD}")) {
			sdf = new SimpleDateFormat("yyyyMMdd");
			// plus dyas
			if (offRange != null) {
				int off = 0;
				if (cycleUnit.equals("D")) {
					if ("0".equals(offRange)) {
						c.add(Calendar.DAY_OF_MONTH, 1);
					}
				}
				else if (cycleUnit.equals("W")) {
					// this week
					off = 7 + (Integer.parseInt(day) - 1) - Integer.parseInt(offRange);
					c.add(Calendar.DAY_OF_WEEK, off);
				}
				else if (cycleUnit.equals("M")) {
					c.add(Calendar.MONTH, 1);
					off = (Integer.parseInt(day) - 1) - Integer.parseInt(offRange);
					c.add(Calendar.DAY_OF_MONTH, off);
				}
			}
		}
		else if (timeParams.equals("${YYYYMMDDHH}")) {
			sdf = new SimpleDateFormat("yyyyMMddHH");
			// plus hours
			if (offRange != null) {   
				if (cycleUnit.equals("H")) {
					if ("0".equals(offRange)) {
						c.add(Calendar.HOUR_OF_DAY, 1);
					}
				}
				else {
					System.out.println("---cycuit!=HOUR;yyyymmDDHH ;offrange=" + offRange);
				}
			}
		}else if (timeParams.equals("${YYYYMMDDHHmm}")){
			sdf = new SimpleDateFormat("yyyyMMddHHmm");
			// plus hours
			if (offRange != null) {
				if (cycleUnit.equals("I")) {
					if ("0".equals(offRange)) {
						c.add(Calendar.MINUTE, 1);
					}
				}
				else {
					System.out.println("---cycuit!=MINUTE;yyyymmDDHHmm ;offrange=" + offRange);
				}
			}
		}else if(timeParams.equals("${YYYYMMDDHHmmss}")){
			sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			// plus hours
			if (offRange != null) {
				if (cycleUnit.equals("S")) {
					if ("0".equals(offRange)) {
						c.add(Calendar.SECOND, 1);
					}
				}
				else {
					System.out.println("---cycuit!=SECOND;yyyymmDDHHmmss ;offrange=" + offRange);
				}
			}
		}
		times = sdf.format(c.getTime());
		return times;
	}

	private static int timePlus(String fullParam){
		//plus second
		int offset = 0;
		fullParam = fullParam.substring(2, fullParam.length() - 1);
		if (fullParam.indexOf('-') >= 0) {
			String t[] = fullParam.split("-");
			offset = -Integer.parseInt(t[t.length - 1]);
		}
		else if (fullParam.indexOf('+') >= 0) {
			String t[] = fullParam.split("\\+");
			offset = Integer.parseInt(t[t.length - 1]);
		}

		return offset;
	}
	
	/**
	 * YYYY,YYYYYMM,YYYYMMDD,YYYYMMDDHH <br>
	 * <br>
	 * 
	 * Test:System.out.println(standardizeParams( "$ADD(${YYYY},-15),$ADD(${YYYYMM},+5),$ADD(${YYYYMMDD},-10),
	 * $ADD(${YYYYMMDDHH},3),YYYYMMDD,1,'x x','aa',aS" , System.currentTimeMillis()));
	 * 
	 * yyyymmdd+offRange yyyymmddhh+offRange
	 * 
	 * @param params
	 * @param currentRunDate
	 * @return
	 */
	public static String standardizeParams(String params, long currentRunDate, String offRange, String cycleUnit,
			String dayNum) throws Exception {
		if (params == null || "".equals(params) || currentRunDate == 0)
			return null;
		StringBuffer standardParam = new StringBuffer();
		String timeValue = "";
		// Calendar c = Calendar.getInstance();
		String year = "\\$\\{[Y,y]{4}\\}";
		String month = "\\$\\{[Y,y]{4}[M,m]{2}\\}";
		String day = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}\\}";
		String hour = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}\\}";
		String minute= "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}\\}";
		String second="\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}[S,s]{2}\\}";
		
		String secondPlus="\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}[S,s]{2}\\+?-?[0-9]+\\}";
		String minutePlus="\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}\\+?-?[0-9]+\\}";
		String hourPlus = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}\\+?-?[0-9]+\\}";
		String dayPlus = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}\\+?-?[0-9]+\\}";
		String monthPlus = "\\$\\{[Y,y]{4}[M,m]{2}\\+?-?[0-9]+\\}";
		String yearPlus = "\\$\\{[Y,y]{4}\\+?-?[0-9]+\\}";

		Pattern yP = Pattern.compile(year);
		Pattern ymP = Pattern.compile(month);
		Pattern ymdP = Pattern.compile(day);
		Pattern ymdhP = Pattern.compile(hour);
		Pattern ymdhmP=Pattern.compile(minute);
		Pattern ymdhmsP=Pattern.compile(second);
		
		Pattern yplusP = Pattern.compile(yearPlus);
		Pattern ymplusP = Pattern.compile(monthPlus);
		Pattern ymdplusP = Pattern.compile(dayPlus);
		Pattern ymdhplusP = Pattern.compile(hourPlus);
		Pattern ymdhmplusP=Pattern.compile(minutePlus);
		Pattern ymdhmsplusP=Pattern.compile(secondPlus);

		try {
			String param[] = params.split(",");
			SimpleDateFormat sdf;
			for (int i = 0; i < param.length; i++) {
				String tmp = param[i].trim();
				Matcher yM = yP.matcher(tmp);
				Matcher ymM = ymP.matcher(tmp);
				Matcher ymdM = ymdP.matcher(tmp);
				Matcher ymdhM = ymdhP.matcher(tmp);
				Matcher ymdhmM=ymdhmP.matcher(tmp);
				Matcher ymdhmsM=ymdhmsP.matcher(tmp);

				Matcher yplusM = yplusP.matcher(tmp);
				Matcher ymplusM = ymplusP.matcher(tmp);
				Matcher ymdplusM = ymdplusP.matcher(tmp);
				Matcher ymdhplusM = ymdhplusP.matcher(tmp);
				Matcher ymdhmplusM=ymdhmplusP.matcher(tmp);
				Matcher ymdhmsplusM=ymdhmsplusP.matcher(tmp);

				if (yM.find()) {
					standardParam.append(param[i].replaceAll(year,
							getTaskTime("${YYYY}", currentRunDate, offRange, cycleUnit, dayNum)));
				}
				else if (ymM.find()) {
					standardParam.append(param[i].replaceAll(month,
							getTaskTime("${YYYYMM}", currentRunDate, offRange, cycleUnit, dayNum)));
				}
				else if (ymdM.find()) {
					standardParam.append(param[i].replaceAll(day,
							getTaskTime("${YYYYMMDD}", currentRunDate, offRange, cycleUnit, dayNum)));
				}
				else if (ymdhM.find()) {
					standardParam.append(param[i].replaceAll(hour,
							getTaskTime("${YYYYMMDDHH}", currentRunDate, offRange, cycleUnit, dayNum)));
				}else if(ymdhmM.find()){
					standardParam.append(param[i].replaceAll(minute,
							getTaskTime("${YYYYMMDDHHmm}", currentRunDate, offRange, cycleUnit, dayNum)));
				}else if(ymdhmsM.find()){
					standardParam.append(param[i].replaceAll(second,
							getTaskTime("${YYYYMMDDHHmmss}", currentRunDate, offRange, cycleUnit, dayNum)));
				}else if(ymdhmsplusM.find()){
					//plus second
					sdf = new SimpleDateFormat("yyyyMMddHHmmss");
					Calendar c1 = Calendar.getInstance();
					c1.setTime(sdf.parse(getTaskTime("${YYYYMMDDHHmmss}", currentRunDate, offRange, cycleUnit, dayNum)));
					String fullParam = ymdhmsplusM.group(0);
					c1.add(Calendar.SECOND,timePlus(fullParam));
					timeValue=sdf.format(c1.getTime());
					standardParam.append(ymdhmsplusM.replaceAll(timeValue));
				}else if(ymdhmplusM.find()){
					//plus minute
					sdf = new SimpleDateFormat("yyyyMMddHHmm");
					Calendar c2 = Calendar.getInstance();
					c2.setTime(sdf.parse(getTaskTime("${YYYYMMDDHHmm}", currentRunDate, offRange, cycleUnit, dayNum)));
					String fullParam = ymdhmplusM.group(0);
					c2.add(Calendar.MINUTE,timePlus(fullParam));
					timeValue=sdf.format(c2.getTime());
					standardParam.append(ymdhmplusM.replaceAll(timeValue));
				}
				else if (ymdhplusM.find()) {
					// plus hours
					sdf = new SimpleDateFormat("yyyyMMddHH");
					Calendar c3 = Calendar.getInstance();
					c3.setTime(sdf.parse(getTaskTime("${YYYYMMDDHH}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = ymdhplusM.group(0);
					c3.add(Calendar.HOUR_OF_DAY,timePlus(fullParam));
					timeValue=sdf.format(c3.getTime());
					standardParam.append(ymdhplusM.replaceAll(timeValue));

				}
				else if (ymdplusM.find()) {
					// plus days
					sdf = new SimpleDateFormat("yyyyMMdd");
					Calendar c4 = Calendar.getInstance();
					c4.setTime(sdf.parse(getTaskTime("${YYYYMMDD}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = ymdplusM.group(0);
					c4.add(Calendar.DAY_OF_MONTH,timePlus(fullParam));
					timeValue=sdf.format(c4.getTime());
					standardParam.append(ymdplusM.replaceAll(timeValue));

				}
				else if (ymplusM.find()) {
					// month plus
					Calendar c5 = Calendar.getInstance();
					sdf = new SimpleDateFormat("yyyyMM");
					c5.setTime(sdf.parse(getTaskTime("${YYYYMM}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = ymplusM.group(0);
					c5.add(Calendar.MONTH,timePlus(fullParam));
					timeValue=sdf.format(c5.getTime());
					standardParam.append(ymplusM.replaceAll(timeValue));
				}
				else if (yplusM.find()) {
					sdf = new SimpleDateFormat("yyyy");
					Calendar c6 = Calendar.getInstance();
					c6.setTime(sdf.parse(getTaskTime("${YYYY}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = yplusM.group(0);
					c6.add(Calendar.YEAR,timePlus(fullParam));
					timeValue=sdf.format(c6.getTime());
					standardParam.append(yplusM.replaceAll(timeValue));
				}
				else {
					standardParam.append(tmp);
				}
				if (i != param.length - 1)
					standardParam.append(" ");
			}
		}
		catch (Exception e) {
			throw new Exception(e);
		}
		return standardParam.toString();
	}

	public static String standardizeParams(String params, long currentRunDate, String offRange, String cycleUnit,
			String dayNum, String concatChar) throws Exception {
		if (params == null || "".equals(params) || currentRunDate == 0)
			return null;
		StringBuffer standardParam = new StringBuffer();
		String timeValue = "";
		// Calendar c = Calendar.getInstance();
		String year = "\\$\\{[Y,y]{4}\\}";
		String month = "\\$\\{[Y,y]{4}[M,m]{2}\\}";
		String day = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}\\}";
		String hour = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}\\}";
		String minute= "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}\\}";
		String second="\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}[S,s]{2}\\}";
		
		String secondPlus="\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}[S,s]{2}\\+?-?[0-9]+\\}";
		String minutePlus="\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}[F,f]{2}\\+?-?[0-9]+\\}";
		String hourPlus = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}[H,h]{2}\\+?-?[0-9]+\\}";
		String dayPlus = "\\$\\{[Y,y]{4}[M,m]{2}[D,d]{2}\\+?-?[0-9]+\\}";
		String monthPlus = "\\$\\{[Y,y]{4}[M,m]{2}\\+?-?[0-9]+\\}";
		String yearPlus = "\\$\\{[Y,y]{4}\\+?-?[0-9]+\\}";

		Pattern yP = Pattern.compile(year);
		Pattern ymP = Pattern.compile(month);
		Pattern ymdP = Pattern.compile(day);
		Pattern ymdhP = Pattern.compile(hour);
		Pattern ymdhmP=Pattern.compile(minute);
		Pattern ymdhmsP=Pattern.compile(second);
		
		Pattern yplusP = Pattern.compile(yearPlus);
		Pattern ymplusP = Pattern.compile(monthPlus);
		Pattern ymdplusP = Pattern.compile(dayPlus);
		Pattern ymdhplusP = Pattern.compile(hourPlus);
		Pattern ymdhmplusP=Pattern.compile(minutePlus);
		Pattern ymdhmsplusP=Pattern.compile(secondPlus);

		try {
			String param[] = params.split(",");
			SimpleDateFormat sdf;
			for (int i = 0; i < param.length; i++) {
				String tmp = param[i].trim();
				Matcher yM = yP.matcher(tmp);
				Matcher ymM = ymP.matcher(tmp);
				Matcher ymdM = ymdP.matcher(tmp);
				Matcher ymdhM = ymdhP.matcher(tmp);
				Matcher ymdhmM=ymdhmP.matcher(tmp);
				Matcher ymdhmsM=ymdhmsP.matcher(tmp);

				Matcher yplusM = yplusP.matcher(tmp);
				Matcher ymplusM = ymplusP.matcher(tmp);
				Matcher ymdplusM = ymdplusP.matcher(tmp);
				Matcher ymdhplusM = ymdhplusP.matcher(tmp);
				Matcher ymdhmplusM=ymdhmplusP.matcher(tmp);
				Matcher ymdhmsplusM=ymdhmsplusP.matcher(tmp);

				if (yM.find()) {
					standardParam.append(param[i].replaceAll(year,
							getTaskTime("${YYYY}", currentRunDate, offRange, cycleUnit, dayNum)));
				}
				else if (ymM.find()) {
					standardParam.append(param[i].replaceAll(month,
							getTaskTime("${YYYYMM}", currentRunDate, offRange, cycleUnit, dayNum)));
				}
				else if (ymdM.find()) {
					standardParam.append(param[i].replaceAll(day,
							getTaskTime("${YYYYMMDD}", currentRunDate, offRange, cycleUnit, dayNum)));
				}
				else if (ymdhM.find()) {
					standardParam.append(param[i].replaceAll(hour,
							getTaskTime("${YYYYMMDDHH}", currentRunDate, offRange, cycleUnit, dayNum)));
				}else if(ymdhmM.find()){
					standardParam.append(param[i].replaceAll(minute,
							getTaskTime("${YYYYMMDDHHmm}", currentRunDate, offRange, cycleUnit, dayNum)));
				}else if(ymdhmsM.find()){
					standardParam.append(param[i].replaceAll(second,
							getTaskTime("${YYYYMMDDHHmmss}", currentRunDate, offRange, cycleUnit, dayNum)));
				}else if(ymdhmsplusM.find()){
					//plus second
					sdf = new SimpleDateFormat("yyyyMMddHHmmss");
					Calendar c1 = Calendar.getInstance();
					c1.setTime(sdf.parse(getTaskTime("${YYYYMMDDHHmmss}", currentRunDate, offRange, cycleUnit, dayNum)));
					String fullParam = ymdhmsplusM.group(0);
					c1.add(Calendar.SECOND,timePlus(fullParam));
					timeValue=sdf.format(c1.getTime());
					standardParam.append(ymdhmsplusM.replaceAll(timeValue));
				}else if(ymdhmplusM.find()){
					//plus minute
					sdf = new SimpleDateFormat("yyyyMMddHHmm");
					Calendar c2 = Calendar.getInstance();
					c2.setTime(sdf.parse(getTaskTime("${YYYYMMDDHHmm}", currentRunDate, offRange, cycleUnit, dayNum)));
					String fullParam = ymdhmplusM.group(0);
					c2.add(Calendar.MINUTE,timePlus(fullParam));
					timeValue=sdf.format(c2.getTime());
					standardParam.append(ymdhmplusM.replaceAll(timeValue));
				}
				else if (ymdhplusM.find()) {
					// plus hours
					sdf = new SimpleDateFormat("yyyyMMddHH");
					Calendar c3 = Calendar.getInstance();
					c3.setTime(sdf.parse(getTaskTime("${YYYYMMDDHH}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = ymdhplusM.group(0);
					c3.add(Calendar.HOUR_OF_DAY,timePlus(fullParam));
					timeValue=sdf.format(c3.getTime());
					standardParam.append(ymdhplusM.replaceAll(timeValue));

				}
				else if (ymdplusM.find()) {
					// plus days
					sdf = new SimpleDateFormat("yyyyMMdd");
					Calendar c4 = Calendar.getInstance();
					c4.setTime(sdf.parse(getTaskTime("${YYYYMMDD}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = ymdplusM.group(0);
					c4.add(Calendar.DAY_OF_MONTH,timePlus(fullParam));
					timeValue=sdf.format(c4.getTime());
					standardParam.append(ymdplusM.replaceAll(timeValue));

				}
				else if (ymplusM.find()) {
					// month plus
					Calendar c5 = Calendar.getInstance();
					sdf = new SimpleDateFormat("yyyyMM");
					c5.setTime(sdf.parse(getTaskTime("${YYYYMM}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = ymplusM.group(0);
					c5.add(Calendar.MONTH,timePlus(fullParam));
					timeValue=sdf.format(c5.getTime());
					standardParam.append(ymplusM.replaceAll(timeValue));
				}
				else if (yplusM.find()) {
					sdf = new SimpleDateFormat("yyyy");
					Calendar c6 = Calendar.getInstance();
					c6.setTime(sdf.parse(getTaskTime("${YYYY}", currentRunDate, offRange, cycleUnit, dayNum)));

					String fullParam = yplusM.group(0);
					c6.add(Calendar.YEAR,timePlus(fullParam));
					timeValue=sdf.format(c6.getTime());
					standardParam.append(yplusM.replaceAll(timeValue));
				}
				else {
					standardParam.append(tmp);
				}
				if (i != param.length - 1)
					standardParam.append(concatChar);
			}
		}
		catch (Exception e) {
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
	public static String standardizeDateString(String str, long currentRunDate) throws Exception {
		if (str == null || currentRunDate == 0)
			return null;

		if ("".equals(str))
			return "";

		String rep1 = new SimpleDateFormat("yyyy").format(currentRunDate);
		String rep2 = new SimpleDateFormat("yyyy-MM").format(currentRunDate);
		String rep3 = new SimpleDateFormat("yyyy-MM-dd").format(currentRunDate);
		String rep4 = new SimpleDateFormat("yyyyMMddHH").format(currentRunDate);

		str = str.replaceAll("\\[YYYYMMDDHH\\]", "YYYYMMDDHH");
		str = str.replaceAll("\\[YYYYMMDD\\]", "YYYY-MM-DD");
		str = str.replaceAll("\\[YYYYMM\\]", "YYYY-MM");
		str = str.replaceAll("\\[YYYY\\]", "YYYY");

		str = str.replaceAll("\\$\\{YYYYMMDDHH\\}", rep4);
		str = str.replaceAll("\\$\\{YYYY-MM-DD\\}", rep3);
		str = str.replaceAll("\\$\\{YYYY-MM\\}", rep2);
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
		}
		catch (NumberFormatException e) {
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
		}
		else {
			/* Delete all the files in the destination directory first. */
			File[] allDestFiles = deleteFile.listFiles();
			for (File destFile : allDestFiles) {
				destFile.delete();
			}

			deleteFile.delete();
		}
	}

	public static void deleteHdfsFile(FileSystem hdfs, Path deleteFile) throws IOException {

		if (!hdfs.exists(deleteFile)) {
			return;
		}

		FileStatus destPathStatus = hdfs.getFileStatus(deleteFile);
		hdfs.delete(deleteFile, destPathStatus.isDir());
	}

	/* Remove the TDW data with the session id, for those redo task. */
	public static boolean removeHDFSFilesWithPattern(String nameNodeUser, String nameNodeGroup, String nameNodeIP,
			int nameNodePort, String sessionId) {
		return false;
	}

	/* Create the HDFS temporary path for HDFS, LINUX load in task. */
	public static boolean createTempHDFSPath(String nameNodeUser, String nameNodeGroup, String nameNodeIP,
			int nameNodePort, String tmpPath) {
		FileSystem fs = null;
		boolean success = false;
		try {
			Configuration configuration = new Configuration();
			configuration.set("fs.default.name", "hdfs://" + nameNodeIP + ":" + nameNodePort);
			configuration.set("hadoop.job.ugi", nameNodeUser + "," + nameNodeGroup);
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
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (fs != null) {
				try {
					fs.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return success;
	}
}
