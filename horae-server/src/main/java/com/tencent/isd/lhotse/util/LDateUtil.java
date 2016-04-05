package com.tencent.isd.lhotse.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class LDateUtil {
	public static final String MINUTE_CYCLE = "I";
	public static final String HOUR_CYCLE = "H";
	public static final String DAY_CYCLE = "D";
	public static final String WEEK_CYCLE = "W";
	public static final String MONTH_CYCLE = "M";
	private static final SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	private static final SimpleDateFormat RichDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	private static final SimpleDateFormat UTCDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) {
		System.out.println(getCurrentTimestamp());
	}

	/**
	 * generate timestamp like '20120501132518123'
	 * 
	 * @return
	 */
	public static synchronized String getCurrentTimestamp() {
		return RichDateFormat.format(new Date());
	}

	public static synchronized String getCurrentTimeString() {
		return FullDateFormat.format(new Date());
	}

	public static synchronized String getFullDateString(Date dt) {
		return FullDateFormat.format(dt);
	}

	public static synchronized String getUTCDateString(Date dt) {
		return UTCDateFormat.format(dt);
	}

	public static synchronized String getDateString(Date dt, int len) {
		if ((len < 6) || (len > 14)) {
			return null;
		}
		String fullStr = FullDateFormat.format(dt);
		return fullStr.substring(0, len);
	}

	public static synchronized Date getDate(String strDate) throws ParseException {
		String newDateString = formatDateString(strDate.trim());
		return FullDateFormat.parse(newDateString);
	}

	public synchronized static String formatDateString(String strDate) {
		return formatDateString(strDate, 14);
	}

	public synchronized static String formatDateString(String strDate, int len) {
		if ((strDate == null) || (strDate.trim().equals("")) || (strDate.trim().length() < 6)) {
			return null;
		}

		String str = strDate.trim().replaceAll("'", "");
		int len2 = str.length();
		if (len2 <= 6) {
			str = str + "01000000";
		}
		else {
			str = str + "000000";
		}

		return str.substring(0, len);
	}

	public synchronized static Date getDateAfter(Date dt, String cycleType, int amount) {
		Calendar c = Calendar.getInstance();
		c.setTime(dt);
		if (MINUTE_CYCLE.equalsIgnoreCase(cycleType)) {
			c.add(Calendar.MINUTE, amount);
			return c.getTime();
		}
		else if (HOUR_CYCLE.equalsIgnoreCase(cycleType)) {
			c.add(Calendar.HOUR_OF_DAY, amount);
			return c.getTime();
		}
		else if (DAY_CYCLE.equalsIgnoreCase(cycleType)) {
			c.add(Calendar.DAY_OF_YEAR, amount);
			return c.getTime();
		}
		else if (WEEK_CYCLE.equalsIgnoreCase(cycleType)) {
			c.add(Calendar.DAY_OF_YEAR, amount * 7);
			return c.getTime();
		}
		else if (MONTH_CYCLE.equalsIgnoreCase(cycleType)) {
			c.add(Calendar.MONTH, amount);
			return c.getTime();
		}
		return dt;
	}

	public static synchronized Date truncateDate(Date dt, String cycleType) {
		Calendar c = Calendar.getInstance();
		c.setTime(dt);
		// c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		if (HOUR_CYCLE.equalsIgnoreCase(cycleType)) {
			c.set(Calendar.MINUTE, 0);
		}
		else if (DAY_CYCLE.equalsIgnoreCase(cycleType)) {
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
		}
		else if (WEEK_CYCLE.equalsIgnoreCase(cycleType)) {
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.DAY_OF_WEEK, 2);
		}
		else if (MONTH_CYCLE.equalsIgnoreCase(cycleType)) {
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.DAY_OF_MONTH, 1);
		}
		return c.getTime();
	}

	public synchronized static String truncateDate(String dt, String cycleType, int len) throws ParseException {
		return getDateString(truncateDate(getDate(dt), cycleType), len);
	}

	public synchronized static String getDateStringAfter(Date dt, String cycleType, int amount, int len)
			throws ParseException {
		Date nextDate = getDateAfter(dt, cycleType, amount);
		return getDateString(nextDate, len);
	}

	public synchronized static String getDateStringAfter(String strDate, String cycleType, int amount, int len)
			throws ParseException {
		Date dt = getDate(strDate);
		Date nextDate = getDateAfter(dt, cycleType, amount);
		return getDateString(nextDate, len);
	}

	public static String getNextMinuteString(String strDate) throws ParseException {
		return getDateStringAfter(strDate, MINUTE_CYCLE, 1, 12);
	}

	public static String getNextMinuteString(Date dt) throws ParseException {
		Date dtNext = getDateAfter(dt, MINUTE_CYCLE, 1);
		return getDateString(dtNext, 12);
	}

	public static Date getNextMinute(Date dt) throws ParseException {
		return getDateAfter(dt, MINUTE_CYCLE, 1);
	}

	public static String getNextHourString(String strDate) throws ParseException {
		return getDateStringAfter(strDate, HOUR_CYCLE, 1, 10);
	}

	public static String getNextHourString(Date dt) throws ParseException {
		Date dtNext = getDateAfter(dt, HOUR_CYCLE, 1);
		return getDateString(dtNext, 10);
	}

	public static Date getNextHour(Date dt) throws ParseException {
		return getDateAfter(dt, HOUR_CYCLE, 1);
	}

	public static String getNextDayString(String strDate) throws ParseException {
		return getDateStringAfter(strDate, DAY_CYCLE, 1, 8);
	}

	public static String getNextDayString(Date dt) throws ParseException {
		Date dtNext = getDateAfter(dt, DAY_CYCLE, 1);
		return getDateString(dtNext, 8);
	}

	public static Date getNextDay(Date dt) throws ParseException {
		return getDateAfter(dt, DAY_CYCLE, 1);
	}

	public static String getNextWeekString(String strDate) throws ParseException {
		return getDateStringAfter(strDate, WEEK_CYCLE, 1, 8);
	}

	public static String getNextWeekString(Date dt) throws ParseException {
		Date dtNext = getDateAfter(dt, WEEK_CYCLE, 1);
		return getDateString(dtNext, 8);
	}

	public static Date getNextWeek(Date dt) throws ParseException {
		return getDateAfter(dt, WEEK_CYCLE, 1);
	}

	public static String getNextMonthString(String strDate) throws ParseException {
		return getDateStringAfter(strDate, MONTH_CYCLE, 1, 8);
	}

	public static String getNextMonthString(Date dt) throws ParseException {
		Date dtNext = getDateAfter(dt, MONTH_CYCLE, 1);
		return getDateString(dtNext, 8);
	}

	public static Date getNextMonth(Date dt) throws ParseException {
		return getDateAfter(dt, MONTH_CYCLE, 1);
	}

	public synchronized static int compare(String str1, String str2) throws ParseException {
		Date dt1 = getDate(str1);
		Date dt2 = getDate(str2);
		return dt1.compareTo(dt2);
	}

	/**
	 * Convert datetime using a specified format to Java timestamp
	 * 
	 * @param datetime
	 *            datetime string, such as: 2012-07-13 07:13:01
	 * @param dateFormat
	 *            format of datetime, such as: yyyy-MM-dd HH:mm:ss
	 * @return a Java timestamp
	 */
	public synchronized static long datetimeToTimestamp(String datetime, String dateFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		long timestamp = 0L;
		try {
			Date dt = sdf.parse(datetime);
			timestamp = dt.getTime();
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
		return timestamp;
	}

	/**
	 * Convert a Java timestamp using specified format to datetime
	 * 
	 * @param timestamp
	 *            a Java timestamp, such as: 1335368935990
	 * @param dateFormat
	 *            format of datetime, such as: yyyy-MM-dd HH:mm:ss
	 * @return a datetime string
	 */
	public synchronized static String timestampToDatetime(long timestamp, String dateFormat) {
		Date dt = new Date(timestamp);
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(dt);
	}

	/**
	 * Replace time variable(s) using a given datetime
	 * 
	 * @param varStr
	 *            a string contains time variable
	 * @param datetime
	 *            a datetime with format: yyyyMMddHHmmss
	 * @return a string after time variable(s) replaced
	 */
	public synchronized static String replaceTimeVariable(String varStr, String datetime) {
		String replacedStr = varStr;

		// get time scale
		String year = datetime.substring(0, 4);
		String month = datetime.substring(4, 6);
		String day = datetime.substring(6, 8);
		String hour = datetime.substring(8, 10);
		String minute = datetime.substring(10, 12);
		String second = datetime.substring(12, 14);

		// set value of time variable(s)
		HashMap<String, String> timeFormatMap = new HashMap<String, String>();
		timeFormatMap.put(TIME_VAR_YEAR, year);
		timeFormatMap.put(TIME_VAR_MONTH, year + month);
		timeFormatMap.put(TIME_VAR_DAY, year + month + day);
		timeFormatMap.put(TIME_VAR_HOUR, year + month + day + hour);
		timeFormatMap.put(TIME_VAR_MINUTE, year + month + day + hour + minute);
		timeFormatMap.put(TIME_VAR_SECOND, year + month + day + hour + minute + second);

		// replace time variable(s)
		Pattern pt = Pattern.compile("\\$\\{YYYY(MM)?(DD)?(HH)?(MI)?(SS)?\\}");
		Matcher mt = pt.matcher(replacedStr);
		try {
			while (mt.find()) {
				replacedStr = replacedStr.replace(mt.group(), timeFormatMap.get(mt.group()));
			}
		}
		catch (NullPointerException e) {
			e.printStackTrace();
		}
		return replacedStr;
	}

	/**
	 * Parse time variables in a string using a datetime
	 * 
	 * @param varStr
	 *            a string contains time variables
	 * @param datetime
	 *            a datetime with format: yyyyMMddHHmmss
	 * @return a string after time variables parsed
	 */
	public synchronized static String parseTimeVariable(String varStr, String datetime) {
		// Get time scale
		String year = datetime.substring(0, 4);
		String month = datetime.substring(4, 6);
		String day = datetime.substring(6, 8);
		String hour = datetime.substring(8, 10);
		String minute = datetime.substring(10, 12);
		String second = datetime.substring(12, 14);

		// Hash map to store time variable and parsed value
		HashMap<String, String> timeMap = new HashMap<String, String>();

		// To find and parse time variable in string
		Pattern pt = Pattern.compile(TIME_REGEXP);
		Matcher mt = pt.matcher(varStr);
		String timeKey;
		String timeValue;
		while (mt.find()) {
			timeKey = mt.group();

			// Remove keywords ($, {, }) and space
			timeValue = timeKey.replaceAll(KEY_PUNC_REGEXP, EMPTY_STRING);

			if ((timeKey.indexOf(PUNC_PLUS) == -1) && (timeKey.indexOf(PUNC_MINUS) == -1)) {
				// No time operation found, parse time variables directly
				timeValue = timeValue.replace(TIME_SCALE_YEAR, year).replace(TIME_SCALE_MONTH, month)
						.replace(TIME_SCALE_DAY, day).replace(TIME_SCALE_HOUR, hour).replace(TIME_SCALE_MINUTE, minute)
						.replace(TIME_SCALE_SECOND, second);
			}
			else {
				// Time operation found
				String[] fragments = timeValue.split(TIME_OPER_REGEXP);

				// Get time offset
				int offset = Integer.parseInt(fragments[1]);
				if (timeValue.indexOf(PUNC_MINUS) != -1) {
					offset = -1 * offset;
				}
				timeValue = fragments[0];

				// Build a GregorianCalendar to do time operation
				GregorianCalendar gc = new GregorianCalendar(Integer.parseInt(year), Integer.parseInt(month) - 1,
						Integer.parseInt(day), Integer.parseInt(hour), Integer.parseInt(minute),
						Integer.parseInt(second));
				if (timeValue.endsWith(TIME_SCALE_YEAR)) {
					gc.add(Calendar.YEAR, offset);
				}
				else if (timeValue.endsWith(TIME_SCALE_MONTH)) {
					gc.add(Calendar.MONTH, offset);
				}
				else if (timeValue.endsWith(TIME_SCALE_DAY)) {
					gc.add(Calendar.DAY_OF_MONTH, offset);
				}
				else if (timeValue.endsWith(TIME_SCALE_HOUR)) {
					gc.add(Calendar.HOUR_OF_DAY, offset);
				}
				else if (timeValue.endsWith(TIME_SCALE_MINUTE)) {
					gc.add(Calendar.MINUTE, offset);
				}
				else if (timeValue.endsWith(TIME_SCALE_SECOND)) {
					gc.add(Calendar.SECOND, offset);
				}

				// Replace time variable using regular time format in JAVA
				timeValue = timeValue.replace(TIME_SCALE_YEAR, TIME_FORMAT_YEAR)
						.replace(TIME_SCALE_DAY, TIME_FORMAT_DAY).replace(TIME_SCALE_MINUTE, TIME_FORMAT_MINUTE)
						.replace(TIME_SCALE_SECOND, TIME_FORMAT_SECOND);
				SimpleDateFormat sdf = new SimpleDateFormat(timeValue);
				timeValue = sdf.format(gc.getTime());
			}

			// Collect key and value after parsing
			timeMap.put(timeKey, timeValue);
		}
		String parsedStr = varStr;
		Iterator<Map.Entry<String, String>> timeIter = timeMap.entrySet().iterator();
		while (timeIter.hasNext()) {
			// Key stores original time variable in string, value stores real
			// time that parsed
			Map.Entry<String, String> timeEntry = timeIter.next();
			parsedStr = parsedStr.replace(timeEntry.getKey(), timeEntry.getValue());
		}
		return parsedStr;
	}

	public static final String TIME_VAR_YEAR = "${YYYY}";
	public static final String TIME_VAR_MONTH = "${YYYYMM}";
	public static final String TIME_VAR_DAY = "${YYYYMMDD}";
	public static final String TIME_VAR_HOUR = "${YYYYMMDDHH}";
	public static final String TIME_VAR_MINUTE = "${YYYYMMDDHHMI}";
	public static final String TIME_VAR_SECOND = "${YYYYMMDDHHMISS}";
	public static final String TIME_SCALE_YEAR = "YYYY";
	public static final String TIME_SCALE_MONTH = "MM";
	public static final String TIME_SCALE_DAY = "DD";
	public static final String TIME_SCALE_HOUR = "HH";
	public static final String TIME_SCALE_MINUTE = "MI";
	public static final String TIME_SCALE_SECOND = "SS";
	private static final String TIME_FORMAT_YEAR = "yyyy";
	private static final String TIME_FORMAT_DAY = "dd";
	private static final String TIME_FORMAT_MINUTE = "mm";
	private static final String TIME_FORMAT_SECOND = "ss";
	private static final String EMPTY_STRING = "";
	private static final String PUNC_PLUS = "+";
	private static final String PUNC_MINUS = "-";
	private static final String TIME_OPER_REGEXP = "\\+|\\-";

	// Regular expression to match $, {, } and space
	private static final String KEY_PUNC_REGEXP = "\\$|\\{|\\}|\\s";

	// Regular expression to match time variable
	private static final String TIME_REGEXP = "\\$\\{" + "(YYYYMMDDHHMISS|" + "YYYYMMDDHHMI|MMDDHHMISS|"
			+ "YYYYMMDDHH|MMDDHHMI|DDHHMISS|" + "YYYYMMDD|MMDDHH|DDHHMI|HHMISS|" + "YYYYMM|MMDD|DDHH|HHMI|MISS|"
			+ "YYYY|MM|DD|HH|MI|SS){1}" + "(\\s*(\\+|\\-)\\s*\\d+)?\\}";
}
