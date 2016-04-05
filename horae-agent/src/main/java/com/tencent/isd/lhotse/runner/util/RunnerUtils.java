package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunnerUtils {
	private static final String DAY_EXPR_PATH = "${YYYYMMDD}";
	private static final String HOUR_EXPR_PATH = "${YYYYMMDDHH}";
	private static final String MONTH_EXPR_PATH = "${YYYYMM}";
	private static final String MINUTE_EXPR_PATH = "${YYYYMMDDHHFF}";
	private static final String SECOND_EXPR_PATH = "${YYYYMMDDHHFFSS}";
	private static final String ONLY_YEAR_EXPR_PATH = "${YYYY}";
	private static final String ONLY_MONTH_EXPR_PATH = "${MM}";
	private static final String ONLY_DAY_EXPR_PATH = "${DD}";
	private static final String ONLY_HOUR_EXPR_PATH = "${HH}";
	private static final String ONLY_MINUTE_EXPR_PATH = "${FF}";
	private static final String ONLY_SECOND_EXPR_PATH = "${SS}";
	private static final int LAST_LINE_MAX_LENGTH = 3000;
	private static final String DISTCP_PATTERN = "*_distcp_logs*";
	
	public static final String DAY_EXPR = "YYYYMMDD";
	public static final String HOUR_EXPR = "YYYYMMDDHH";
	public static final String MONTH_EXPR = "YYYYMM";	
	public static final String MINUTE_EXPR = "YYYYMMDDHHFF";
	public static final String SECOND_EXPR = "YYYYMMDDHHFFSS";
	
	/* Error codes description. */
	public static final String CONNECT_ERROR_CODE = "3000001";
	public static final String NO_ROUTE_ERROR_CODE = "3000002";
	public static final String PERMISSION_ERROR_CODE = "3000004";
	public static final String DISTCP_ERROR_CODE = "3000012";
	public static final String REMOVE_DISTCP_FILE_ERROR_CODE = "3000013";
	public static final String SERVER_CONFIG_ERROR_CODE = "3000014";
	public static final String ORACLE_TABLE_NAME_ERROR_CODE = "3000015";
	public static final String DATE_FORMAT_ERROR_CODE = "3000016";
	public static final String DELETE_HDFS_FILE_ERROR_CODE = "3000017";	
	public static final String DB_TABLE_NAME_ERROR_CODE = "3000018";
	public static final String UNKOWN_ERROR_CODE = "3000099";
	public static final String HADOOP_SUCCESS_CODE = "3000100";
	public static final String MAX_ROWS_LIMIT_REACHED = "3000101";
	public static final String WRONG_HADOOP_TARGET_PATH = "3000102";
	public static final String NO_CHECK_EXIST_ERROR_CODE = "3000103";
	public static final String COS_COPY_ERROR_CODE = "3000104";
	public static final String REMOVE_COS_FILE_ERROR_CODE = "3000105";
	public static final String CREATE_COS_FILE_ERROR_CODE = "3000106";
	public static final String BI_DB_ERROR_CODE = "3000107";
	public static final String CHECK_FILE_NOT_EXIST_CODE = "8110109";
	public static final String LOAD_ZERO_OUT_ERROR_CODE = "8110110";
	public static final String TDW_NO_DB_ERROR_CODE = "TDW-00201";
	public static final String TDW_NO_TABLE_ERROR_CODE = "TDW-00202";
	public static final String TDW_NO_PARTITION_ERROR_CODE = "TDW-00203";
	public static final String TDW_CLEAR_OLD_DATA_ERROR_CODE = "TDW-00204";
	public static final String PIG_RUN_DEFAULT_ERROR_CODE = "PIG-00001";
	public static final int SUCCESS_CODE = 0;
	
	/* Error code key description. */
	public static final String CONNECT_KEY = "Bad connection to FS. command aborted";
	public static final String NO_ROUTE_KEY_1 = "unknown host";
	public static final String NO_ROUTE_KEY_2 = "No route to host";
	public static final String PERMISSION_KEY = "permission denied";
	
	/* For support ${YYYYMMDD-1D}*/
	private static final String STR_PATTERN = "\\$\\{(YYYY)?(MM)?(DD)?(HH)?(FF)?(SS)?([\\-\\+][0-9]+[MDHFS]*)*\\}";
	private static final Pattern stringPat = Pattern.compile(STR_PATTERN);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	protected static final HashMap<String, Integer> DATEFORMAT = new HashMap<String, Integer>() {
		private static final long serialVersionUID = 587L;
		{
			put("M", Calendar.MONTH);
			put("D", Calendar.DAY_OF_MONTH);
			put("H", Calendar.HOUR);
			put("F", Calendar.MINUTE);
			put("S", Calendar.SECOND);
		}
	};

	/* Replace the date regression string in the path. */
	public static String replaceDateExpr(String originalExpr, Date runDate) {
		if (originalExpr == null) {
			return null;
		}

		String year = new SimpleDateFormat("yyyy").format(runDate);
		String month = new SimpleDateFormat("MM").format(runDate);
		String day = new SimpleDateFormat("dd").format(runDate);
		String hour = new SimpleDateFormat("HH").format(runDate);
		String minute = new SimpleDateFormat("mm").format(runDate);
		String second = new SimpleDateFormat("ss").format(runDate);

		/* Replace those date expression in the path. */
		originalExpr = originalExpr.replace(SECOND_EXPR_PATH, year + month + day + hour + minute + second);
		originalExpr = originalExpr.replace(MINUTE_EXPR_PATH, year + month + day + hour + minute);
		originalExpr = originalExpr.replace(HOUR_EXPR_PATH, year + month + day + hour);
		originalExpr = originalExpr.replace(DAY_EXPR_PATH, year + month + day);
		originalExpr = originalExpr.replace(MONTH_EXPR_PATH, year + month);

		/* Just for those not regular regressions. */
		originalExpr = originalExpr.replace(ONLY_SECOND_EXPR_PATH, second);
		originalExpr = originalExpr.replace(ONLY_MINUTE_EXPR_PATH, minute);
		originalExpr = originalExpr.replace(ONLY_HOUR_EXPR_PATH, hour);
		originalExpr = originalExpr.replace(ONLY_DAY_EXPR_PATH, day);
		originalExpr = originalExpr.replace(ONLY_MONTH_EXPR_PATH, month);
		originalExpr = originalExpr.replace(ONLY_YEAR_EXPR_PATH, year);

		/* Replace those data expression in the partition expression. */
		originalExpr = originalExpr.replace(HOUR_EXPR, year + month + day + hour);
		originalExpr = originalExpr.replace(DAY_EXPR, year + month + day);
		originalExpr = originalExpr.replace(MONTH_EXPR, year + month);
		originalExpr = originalExpr.replace(MINUTE_EXPR, year + month + day + hour + minute);
		originalExpr = originalExpr.replace(SECOND_EXPR, year + month + day + hour + minute + second);

		if (originalExpr.contains("{") || originalExpr.contains("}")) {
			throw new IllegalArgumentException("Illegal date expression for: " + originalExpr);
		}

		return originalExpr;
	}

	/*get hdfsFileDescriptors from hadoop api to get filesize and filename*/
	/*jessicajin*/
	public static ListHdfsDirResult getHdfsFileDescriptorsApi(LServer hdfsServer, String hdfsPath,
			String hdfsFilePattern, AbstractTaskRunner runner) throws Exception{
		ArrayList<HdfsFileDescriptor> fileDescriptors = new ArrayList<HdfsFileDescriptor>();
		String host = getHostWithProtocol(hdfsServer.getVersion(), hdfsServer.getHost());
		final String fsDefaultName = host + ":" + hdfsServer.getPort();
		String url=fsDefaultName+hdfsPath +File.separator+hdfsFilePattern;
		runner.writeLocalLog(Level.INFO,"hadoop.job.ugi=" +hdfsServer.getUserName() + "," + hdfsServer.getUserGroup()
				+"  "+url+"  filePatter="+hdfsFilePattern);
		
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi",hdfsServer.getUserName() + "," + hdfsServer.getUserGroup());
		
		runner.writeLocalLog(Level.INFO," "+url);
		
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus[] status=fs.globStatus(new Path(url));
		
		for(FileStatus t:status){
			//runner.writeLocalLog(Level.INFO,"hadoop_api:"+ t.getPath().getName()+"  "+t.getLen()+" "+t.getPath());
			HdfsFileDescriptor fileDescriptor = new HdfsFileDescriptor();
			fileDescriptor.setFileName(t.getPath().getName());
			fileDescriptor.setFileSize(t.getLen());
			fileDescriptors.add(fileDescriptor);
		}
		return new ListHdfsDirResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE, fileDescriptors);
	}
	
	public static ListHdfsDirResult getHdfsFileDescriptors(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String hdfsFilePattern, AbstractTaskRunner runner) throws Exception {

		ArrayList<HdfsFileDescriptor> fileDescriptors = new ArrayList<HdfsFileDescriptor>();

		String host = getHostWithProtocol(hdfsServer.getVersion(), hdfsServer.getHost());
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-count",
				hdfsPath + File.separator + hdfsFilePattern };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner, true);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new ListHdfsDirResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()), null);
		}

		for (String line : process.getAllLines()) {
			runner.writeLocalLog(Level.INFO, line);
			HdfsFileDescriptor fileDescriptor = new HdfsFileDescriptor();
			StringTokenizer st = new StringTokenizer(line, " ");
			int counter = 0;
			while (st.hasMoreElements()) {
				counter++;
				String element = st.nextToken().trim();
				if (counter == 3) {
					fileDescriptor.setFileSize(new Long(element));
				}

				if (counter == 4) {
					element = element.substring(element.lastIndexOf('/') + 1, element.length());
					fileDescriptor.setFileName(element);
				}
			}
			fileDescriptors.add(fileDescriptor);
		}
		process.getAllLines().clear();

		return new ListHdfsDirResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE, fileDescriptors);
	}

	public static String getHostWithProtocol(String hdfsServerVersion, String host) {
		if ("IM".equalsIgnoreCase(hdfsServerVersion)) {
			return "hftp://" + host;
		}

		if (!host.startsWith("hdfs://")) {
			return "hdfs://" + host;
		}

		return host;
	}

	public static HdfsDirFileCounter getHdfsDirFileCount(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String hdfsFilePattern, AbstractTaskRunner runner) throws Exception {

		String host = getHostWithProtocol(hdfsServer.getVersion(), hdfsServer.getHost());
		final String fsDefaultName = host + ":" + hdfsServer.getPort();
		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-count",
				hdfsPath + File.separator + hdfsFilePattern };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HdfsDirFileCounter(process.getExitVal(), getHadoopErrorCode(process.getLastLine()), -1);
		}

		return new HdfsDirFileCounter(SUCCESS_CODE, HADOOP_SUCCESS_CODE, process.getLineCount());
	}

	public static void logCommand(String[] cmdArray, AbstractTaskRunner runner) {
		StringBuilder sb = new StringBuilder();
		for (String command : cmdArray) {
			if (!command.contains("hadoop.job.ugi")) {
				sb.append(command + " ");
			}
		}
		runner.writeLocalLog(Level.INFO, "Executing command: " + sb.toString());
	}

	private static String getHadoopErrorCode(String lastLine) {
		if (lastLine == null) {
			return UNKOWN_ERROR_CODE;
		}

		if (lastLine.contains(CONNECT_KEY)) {
			return CONNECT_ERROR_CODE;
		}

		if (lastLine.contains(NO_ROUTE_KEY_1) || lastLine.contains(NO_ROUTE_KEY_2)) {
			return NO_ROUTE_ERROR_CODE;
		}

		if (lastLine.contains(PERMISSION_KEY)) {
			return PERMISSION_ERROR_CODE;
		}

		return UNKOWN_ERROR_CODE;
	}

	/*get the content of check file by constant version api of hadoop*/
	/*jessicajin*/
	public static CatFileResult catHdfsFileApi(LServer hdfsServer,String hdfsPath,String hdfsFileName,
			AbstractTaskRunner runner) throws Exception{
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();
		final String url =fsDefaultName+ hdfsPath + File.separator + hdfsFileName;
		
		runner.writeLocalLog(Level.INFO,"hadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup()+
				"  hdfsPath="+hdfsPath+"  hdfsFileName"+hdfsFileName+"   url="+ url);
		
		
		
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi",hdfsServer.getUserName() + "," + hdfsServer.getUserGroup());

		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FSDataInputStream in=null;
		in = fs.open( new Path(url) );
		
		InputStreamReader isr = new InputStreamReader(in,"utf-8");
		BufferedReader br = new BufferedReader(isr);
		
		ArrayList<String> checkContent = new ArrayList<String>();
		
		String str = "";
		while((str = br.readLine()) != null){
			//runner.writeLocalLog(Level.INFO,str);
			checkContent.add(str);
		}
			
		return new CatFileResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE, checkContent);
	}
	
	public static CatFileResult catHdfsFile(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String hdfsFileName, AbstractTaskRunner runner) throws Exception {

		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-cat",
				hdfsPath + File.separator + hdfsFileName };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner, true);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new CatFileResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()), null);
		}

		return new CatFileResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE, process.getAllLines());
	}

	public static HadoopResult createHdfsCheckFile(String taskId, String runDate, String hadoopCommand,
			LServer hdfsServer, String destFilePath, String tmpCheckFileDir, String checkFilePath,
			String checkFileName, AbstractTaskRunner runner) throws Exception {

		runner.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		ListHdfsDirResult dirResult = getHdfsFileDescriptors(hadoopCommand, hdfsServer, destFilePath, "attempt_*",
				runner);

		if (dirResult.getExitVal() != 0) {
			return dirResult;
		}

		/* Clear the temporary check file if it exists. */
		runner.writeLocalLog(Level.INFO, "************************tmp check path from task type:" + tmpCheckFileDir);
		if (tmpCheckFileDir == null) {
			tmpCheckFileDir = "/data/lhotse/runners/check_file_tmp_dir";
		}
		runner.writeLocalLog(Level.INFO, "************************tmp check path from task type:" + tmpCheckFileDir);
		
		
		File tmpCheckFilePath = new File(tmpCheckFileDir, taskId);
		if (!tmpCheckFilePath.exists()) {
			tmpCheckFilePath.mkdirs();
		}

		tmpCheckFilePath = new File(tmpCheckFilePath, runDate);
		if (!tmpCheckFilePath.exists()) {
			tmpCheckFilePath.mkdirs();
		}

		File tmpCheckFile = new File(tmpCheckFilePath, checkFileName);
		if (tmpCheckFile.exists()) {
			tmpCheckFile.delete();
		}

		/* Create the check file on the local path first. */
		BufferedWriter checkFileWriter = null;
		try {
			checkFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpCheckFile)));
			checkFileWriter.write(dirResult.getDescriptors().size() + "");
			checkFileWriter.newLine();
			for (HdfsFileDescriptor descriptor : dirResult.getDescriptors()) {
				checkFileWriter.write(descriptor.getFileName());
				checkFileWriter.newLine();
			}
			checkFileWriter.flush();
		}
		finally {
			if (checkFileWriter != null) {
				checkFileWriter.close();
			}
		}
		dirResult.getDescriptors().clear();
		dirResult = null;

		HadoopResult result = writeLinuxFileToHdfs(hadoopCommand, hdfsServer, checkFilePath,
				tmpCheckFilePath.getAbsolutePath(), checkFileName, runner);

		/* Remove the whole temporary check file directory. */
		File[] allFiles = tmpCheckFilePath.listFiles();
		for (File allFile : allFiles) {
			allFile.delete();
		}
		tmpCheckFilePath.delete();

		return result;
	}

	public static HdfsDirExistResult isHdfsDirExist(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			AbstractTaskRunner runner) throws Exception {

		return isHdfsFileExist(hadoopCommand, hdfsServer, hdfsPath, null, runner);
	}

	public static HdfsDirExistResult isHdfsFileExist(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String hdfsFileName, AbstractTaskRunner runner) throws Exception {

		String host = getHostWithProtocol(hdfsServer.getVersion(), hdfsServer.getHost());
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-test", "-e",
				(hdfsFileName == null ? hdfsPath : hdfsPath + File.separator + hdfsFileName) };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HdfsDirExistResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()), false);
		}

		return new HdfsDirExistResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE, true);
	}

	/* Check the empty file.This is working for TDBank */
	public static HdfsIsEmptyFileResult isEmptyFileResult(String hadoopCommand, LServer hdfsServer,
			String hdfsAbsolutFileName, AbstractTaskRunner runner) throws Exception {
		String host = getHostWithProtocol(hdfsServer.getVersion(), hdfsServer.getHost());
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-test", "-z",
				hdfsAbsolutFileName };

		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		int exitVal = process.getExitVal();

		if (exitVal == 0) {
			return new HdfsIsEmptyFileResult(exitVal, getHadoopErrorCode(process.getLastLine()), process.getAllLines()
					.toString(), true);
		}
		else if (process.getExitVal() == 1) {
			return new HdfsIsEmptyFileResult(exitVal, getHadoopErrorCode(process.getLastLine()), process.getAllLines()
					.toString(), false);
		}
		else {
			return new HdfsIsEmptyFileResult(exitVal, getHadoopErrorCode(process.getLastLine()), process.getAllLines()
					.toString(), false);
		}
	}

	/* Delete a local Linux file. */
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

	/* Copy HDFS files in a HDFS path to Linux. */
	public static HadoopResult copyHdfsFileToLinux(String hadoopCommand, LServer hdfsServer, String hdfsFilePath,
			String hdfsFileName, File localPath, AbstractTaskRunner runner) throws Exception {

		/* Check whether the local file exists. */
		if (localPath.exists()) {
			File[] localFiles = localPath.listFiles();
			for (File localFile : localFiles) {
				localFile.delete();
			}
			localPath.delete();
		}
		localPath.mkdirs();

		/* Do the actual copy. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-get",
				hdfsFilePath + File.separator + hdfsFileName, localPath.getAbsolutePath() + File.separator };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()));
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}

	/* Write Linux files to HDFS path. */
	public static HadoopResult writeLinuxFileToHdfs(String hadoopCommand, LServer hdfsServer, String hdfsFilePath,
			String localPath, String linuxFileName, AbstractTaskRunner runner) throws Exception {

		/* Do the actual copy. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-put",
				localPath + File.separator + linuxFileName, hdfsFilePath + File.separator };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()));
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}

	/* Delete a HDFS file. */
	public static int deleteHdfsFileRegressive(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			AbstractTaskRunner runner) throws Exception {

		return deleteHdfsFileRegressive(hadoopCommand, hdfsServer, hdfsPath, null, runner);
	}

	public static int deleteHdfsFileRegressive(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String hdfsFileName, AbstractTaskRunner runner) throws Exception {

		/* Return if the directory doesn't exist. */
		HdfsDirExistResult result = isHdfsDirExist(hadoopCommand, hdfsServer, hdfsPath, runner);
		if (!result.isDirExists()) {
			return 0;
		}

		/* Do the actual copy. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-rmr",
				(hdfsFileName == null ? hdfsPath : hdfsPath + File.separator + hdfsFileName) };
		logCommand(cmdArray, runner);

		RunTaskProcess process = new RunTaskProcess(cmdArray, runner);
		process.startProcess(true);
		process.waitAndDestroyProcess();

		return process.getExitVal();
	}

	/* Delete HDFS files with the pattern. */
	public static HadoopResult deleteHdfsFile(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String pattern, AbstractTaskRunner runner) throws Exception {

		runner.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		/* Return if the directory doesn't exist. */
		HdfsDirExistResult result = isHdfsDirExist(hadoopCommand, hdfsServer, hdfsPath, runner);
		if (!result.isDirExists()) {
			return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
		}

		/* Return if the files don't exist. */
		result = isHdfsFileExist(hadoopCommand, hdfsServer, hdfsPath, pattern, runner);
		if (!result.isDirExists()) {
			return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
		}

		/* Do the actual copy. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-rm",
				(pattern == null ? hdfsPath : hdfsPath + File.separator + pattern) };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), DELETE_HDFS_FILE_ERROR_CODE);
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}

	/* Delete those hadoop distcp copy log files. */
	public static int removeHadoopCopyLogFiles(String hadoopCommand, LServer hdfsServer, String filePath,
			AbstractTaskRunner runner) throws Exception {

		return deleteHdfsFileRegressive(hadoopCommand, hdfsServer, filePath, DISTCP_PATTERN, runner);
	}

	/* Create the HDFS temporary path for HDFS, LINUX load in task. */
	public static HadoopResult removeOldPathAndCreateNewPath(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			AbstractTaskRunner runner) throws Exception {

		/* Initiate the variables. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		/* Delete the old temporary path first to clean the old data. */
		HdfsDirExistResult result = isHdfsDirExist(hadoopCommand, hdfsServer, hdfsPath, runner);
		if (result.isDirExists()) {
			final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
					"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-rm",
					hdfsPath + File.separator + "attempt*" };
			logCommand(cmdArray, runner);

			DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
			process.startProcess();
			process.waitAndDestroyProcess();

			if (process.getExitVal() != 0) {
				return new HadoopResult(process.getExitVal(), DELETE_HDFS_FILE_ERROR_CODE);
			}

			return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
		}

		/* Make the new directory. */
		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-mkdir", hdfsPath };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()));
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}

	/* Create the HDFS temporary path for HDFS, LINUX load in task. */
	public static HadoopResult createHDFSPath(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			AbstractTaskRunner runner) throws Exception {

		/* Delete the old temporary path first to clean the old data. */
		HdfsDirExistResult result = isHdfsDirExist(hadoopCommand, hdfsServer, hdfsPath, runner);
		if (result.isDirExists()) {
			return deleteHdfsFile(hadoopCommand, hdfsServer, hdfsPath, "*", runner);
		}

		/* Create the new temporary path. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-mkdir", hdfsPath };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()));
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}

	public static int hadoopMRCopy(String hadoopCommand, LServer sourceServer, LServer targetServer, LTask task,
			String sourceFilePath, String targetFilePath, String haVersionFlag, boolean isLoadingData,
			AbstractTaskRunner runner) throws Exception {

		final String distCopyCommand = "distcp";
		final String sourcePath = "hdfs://" + sourceServer.getHost() + ":" + sourceServer.getPort() + File.separator
				+ sourceFilePath + File.separator;
		final String targetPath = "hdfs://" + targetServer.getHost() + ":" + targetServer.getPort() + File.separator
				+ targetFilePath + File.separator;

		LServer jobTracker = (isLoadingData ? task.getTargetServers(2) : task.getSourceServers(2));

		String[] cmdArray = new String[] { hadoopCommand, distCopyCommand, "-Dtdw.groupname=nrtgroup",
				"-Dhadoop.job.ugi=" + targetServer.getUserName() + "," + targetServer.getUserGroup(),
				"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(), "-update", "-m", "30",
				sourcePath, targetPath };

		runner.writeLocalLog(Level.INFO,
				"HA version flag: " + haVersionFlag + ", Job tracker version: " + jobTracker.getVersion()
						+ ", jobtracker flag: " + jobTracker.getTag());

		if (haVersionFlag.equals(jobTracker.getVersion())) {
			LServer zookeeper = (isLoadingData ? task.getTargetServers(3) : task.getSourceServers(3));
			cmdArray = new String[] { hadoopCommand, distCopyCommand, "-Dtdw.groupname=nrtgroup",
					"-Dhadoop.job.ugi=" + targetServer.getUserName() + "," + targetServer.getUserGroup(),
					"-Dzookeeper.address=" + zookeeper.getHost(),
					"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(), "-update", "-m", "30",
					sourcePath, targetPath };
		}
		logCommand(cmdArray, runner);

		RunTaskProcess distcopyProcess = new RunTaskProcess(cmdArray, runner);
		distcopyProcess.startProcess(true);
		distcopyProcess.waitAndDestroyProcess();

		return distcopyProcess.getExitVal();
	}

	/************************** Below methods are using PLC. *****************************/

	/* Check whether a database exists in the TDW. */
	public static FindOutputResult checkDatabases(String plcProgram, LServer hiveServer, String dbName,
			AbstractTaskRunner runner,String plcParameter) throws Exception {

		final String specialParam=" SET usp.param=" + plcParameter + ";";
		final String showDBCommands ="show databases";

		return returnProcessCheckResult(plcProgram, hiveServer,specialParam+showDBCommands, showDBCommands, dbName, runner);
	}

	/*
	 * Check whether a specified value that you want to find in the process standard output.
	 */
	private static FindOutputResult returnProcessCheckResult(String plcProgram, LServer hiveServer,
			String executeCommand, String commandStart, String expectFindValue, AbstractTaskRunner runner)
			throws Exception {

		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), hiveServer.getPort() + "", executeCommand };
		logPLCCommand(cmdArray, runner);

		CheckProcess process = new CheckProcess(cmdArray, commandStart, expectFindValue, runner);
		process.startProcess(false);
		process.waitAndDestroyProcess();

		runner.writeLocalLog(Level.INFO,"commandStart: "+commandStart+"   executeCommand="+executeCommand+" " +
				"lastLine="+process.getLastLine());
		return new FindOutputResult(process.getExitVal(), getPLCReturnCode(process.getLastLine()),
				process.findCheckValue(), process.getLastLine());
	}

	private static void logPLCCommand(String[] cmdArray, AbstractTaskRunner runner) {
		int counter = 1;
		StringBuilder sb = new StringBuilder();
		for (String command : cmdArray) {
			if ((counter != 2) && (counter != 3)) {
				if (counter == 6) {
					sb.append("\"" + command + "\"");
				}
				else {
					sb.append(command + " ");
				}
			}
			counter++;
		}
		runner.writeLocalLog(Level.INFO, "Executing command: " + sb.toString());
	}

	/* Check whether a table exists in a specified database. */
	public static FindOutputResult checkTables(String plcProgram, LServer hiveServer, String dbName, String tableName,
			AbstractTaskRunner runner,String plcParameter) throws Exception {

		final String specialParam="SET usp.param=" + plcParameter + ";";
		final String useDBCommands = "use " + dbName;
		final String showTableCommands = "show tables '" + tableName + "'";

		return returnProcessCheckResult(plcProgram, hiveServer,specialParam+ useDBCommands + ";" + showTableCommands,
				showTableCommands, tableName, runner);
	}

	/* Check whether a partition exists in a specified table. */
	public static FindOutputResult checkPartitions(String plcProgram, LServer hiveServer, String dbName,
			String tableName, String partitionName, AbstractTaskRunner runner,String plcParameter) throws Exception {

		final String specialParam="SET usp.param=" + plcParameter + ";";
		final String useDBCommands = "use " + dbName;
		final String showPartitionCommands = "show partitions " + tableName;

		return returnProcessCheckResult(plcProgram, hiveServer,specialParam+ useDBCommands + ";" + showPartitionCommands,
				showPartitionCommands, partitionName, runner);
	}

	/*create the partition plcParameter : String  jessicajin*/
	public static PLCResult createPartition(String plcProgram, LServer hiveServer, String dbName, String tableName,
			String partitionName, AbstractTaskRunner runner,String plcParameter) throws Exception {

		final String addPartitionCommand ="SET usp.param=" + plcParameter + ";"+ "USE " + dbName + ";" + "ALTER TABLE " + tableName + " ADD PARTITION "
				+ partitionName + " VALUES IN (" + partitionName.substring(2, partitionName.length()) + ")";

		runner.writeLocalLog(Level.INFO, addPartitionCommand);

		
		
		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), addPartitionCommand };

		return runProcess(cmdArray, runner);
	}
	
	
//	/* Create the partition. */
//	public static PLCResult createPartition(String plcProgram, LServer hiveServer, String dbName, String tableName,
//			String partitionName, AbstractTaskRunner runner) throws Exception {
//
//		final String addPartitionCommand = "USE " + dbName + ";" + "ALTER TABLE " + tableName + " ADD PARTITION "
//				+ partitionName + " VALUES IN (" + partitionName.substring(2, partitionName.length()) + ")";
//
//		runner.writeLocalLog(Level.INFO, addPartitionCommand);
//
//		
//		
//		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
//				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), addPartitionCommand };
//
//		return runProcess(cmdArray, runner);
//	}

	/* Truncate the partition. */
	public static PLCResult truncateData(String plcProgram, LServer hiveServer, String dbName, String tableName,
			String partitionName, AbstractTaskRunner runner,String plcParameter ) throws Exception {

		final String truncateDataCommand ="set usp.param=" + plcParameter + "; USE "
				+ dbName
				+ ";"
				+ ((partitionName == null || partitionName.length() < 2) ? "TRUNCATE TABLE " + tableName
						: "ALTER TABLE " + tableName + " TRUNCATE PARTITION (" + partitionName + ")");

		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), truncateDataCommand };

		return runProcess(cmdArray, runner);
	}

	public static PLCResult createExternalTable(String plcProgram, String plcParameter, LServer hiveServer,
			LServer nameNodeServer, String tempPath, String externalTableDb, String externalTableName,
			String sourceColumnNames, String delimiter, String charSet, AbstractTaskRunner runner) throws Exception {

		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;").append("SET usp.param=" + plcParameter + ";")
				.append("USE " + externalTableDb + ";").append("CREATE EXTERNAL TABLE " + externalTableName + " (");

		StringTokenizer st = new StringTokenizer(sourceColumnNames, ",");
		int countTokens = st.countTokens();
		int counter = 0;
		while (st.hasMoreTokens()) {
			sb.append(st.nextToken().trim() + " string");
			counter++;
			if (counter < countTokens) {
				sb.append(",");
			}
			else {
				sb.append(")");
			}
		}

		/* TODO: replace the delimiter using the ASCII code. */
		sb.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + delimiter + "' LOCATION ")
				.append("'hdfs://" + nameNodeServer.getHost() + ":" + nameNodeServer.getPort() + File.separator
						+ tempPath + "';")
				.append("ALTER TABLE " + externalTableName + " set serdeproperties ('charset'='" + charSet + "');")
				.append("ALTER TABLE " + externalTableName + " set TBLPROPERTIES ('escape.delim'='\1')");

		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), sb.toString() };

		return runProcess(cmdArray, runner);
	}

	private static PLCResult runProcess(String[] cmdArray, AbstractTaskRunner runner) throws Exception {

		logPLCCommand(cmdArray, runner);
		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		return new PLCResult(process.getExitVal(), getPLCReturnCode(process.getLastLine()), process.getLastLine());
	}

	public static String getPLCReturnCode(String lastLine) {
		if (lastLine == null) {
			return null;
		}

		StringTokenizer st = new StringTokenizer(lastLine, " ");
		return st.nextToken();
	}

	public static void setLoadDataInformation(ArrayList<String> stageOutput, LoadDataResult result,
			AbstractTaskRunner runner, boolean loadingData) {
		final String READ_SUCCESS = "READ_SUCCESS_COUNT:";
		final String READ_ERROR = "READ_ERROR_COUNT:";
		final String SINK_SUCCESS = "FILESINK_SUCCESS_COUNT:";
		final String SINK_ERROR = "FILESINK_ERROR_COUNT:";

		long readSuccess = 0;
		long readError = 0;
		long sinkSuccess = 0;
		long sinkError = 0;
		for (String output : stageOutput) {
			output = output.trim();
			runner.writeLocalLog(Level.INFO, "output: " + output);
			if (output.startsWith(SINK_SUCCESS)) {
				sinkSuccess = new Long(output.substring(SINK_SUCCESS.length(), output.length()));
			}

			if (output.startsWith(SINK_ERROR)) {
				sinkError = new Long(output.substring(SINK_ERROR.length(), output.length()));
			}

			if (output.startsWith(READ_SUCCESS)) {
				readSuccess = new Long(output.substring(READ_SUCCESS.length(), output.length()));
			}

			if (output.startsWith(READ_ERROR)) {
				readError = new Long(output.substring(READ_ERROR.length(), output.length()));
			}
		}

		if ((sinkSuccess == 0) && (sinkError == 0)) {
			if (loadingData) {
				result.setSuccessWrite(readSuccess);
				result.setFailWrite(readError);
			}
			else {
				result.setSuccessWrite(0);
				result.setFailWrite(0);
			}
		}
		else {
			result.setSuccessWrite(sinkSuccess);
			result.setFailWrite(sinkError);
		}
	}

	/* Drop the external table. */
	public static PLCResult dropTable(String plcProgram, LServer hiveServer, String databaseName, String tableName,
			AbstractTaskRunner runner,String plcParameter) throws Exception {

		final String dropTableSQL ="Set usp.param=" + plcParameter + "; " +
				"USE " + databaseName + ";" + "DROP TABLE " + tableName;

		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), dropTableSQL };

		return runProcess(cmdArray, runner);
	}

	public static ExplainSQLResult explainSQL(String plcProgram, String plcParameter, LServer hiveServer,
			String databaseName, String filterSQL, AbstractTaskRunner runner) throws Exception {

		/* First explain the filterSQL to get out the columns we need to export. */
		final String explainSQL = "SET tdw.groupname=nrtgroup; SET usp.param=" + plcParameter + "; " + "USE "
				+ databaseName + "; EXPLAIN SELECT * FROM (" + filterSQL + ") GETFILEDTABLE";
		runner.writeLocalLog(Level.WARNING, explainSQL);

		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), explainSQL.toString() };
		logPLCCommand(cmdArray, runner);

		GetFieldNamesProcess process = new GetFieldNamesProcess(cmdArray, runner);
		process.runProcess();

		return new ExplainSQLResult(process.getExitVal(), getPLCReturnCode(process.getLastLine()),
				process.getLastLine(), process.getFieldNames());
	}

	public static PLCResult createExternalTableWithSQL(String plcProgram, String plcParameter, LServer hiveServer,
			LServer nameNodeServer, String externalTableDataPath, String externalTableDb, String externalTableName,
			String destFileDelimiter, ArrayList<String> columnNames, AbstractTaskRunner runner) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append("SET tdw.groupname=nrtgroup;").append("SET usp.param=" + plcParameter + ";")
				.append("USE " + externalTableDb + ";").append("CREATE EXTERNAL TABLE " + externalTableName + "(");

		int counter = 0;
		for (String columnName : columnNames) {
			sb.append(columnName + " string");
			counter++;
			if (counter < columnNames.size()) {
				sb.append(", ");
			}
		}
		sb.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY '")
				.append(destFileDelimiter + "' LOCATION 'hdfs://" + nameNodeServer.getHost())
				.append(":" + nameNodeServer.getPort() + File.separator + externalTableDataPath + "'");

		runner.writeLocalLog(Level.INFO,"createExternalTableWithSQL: "+sb.toString());
		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(), hiveServer.getPassword(),
				hiveServer.getHost(), String.valueOf(hiveServer.getPort()), sb.toString() };

		return runProcess(cmdArray, runner);
	}

	public static class LoadDataResult {
		private String plcReturnCode;
		private int exitVal;
		private long successWrite;
		private long failWrite;

		public void setPLCReturnCode(String plcReturnCode) {
			this.plcReturnCode = plcReturnCode;
		}

		public void setExitVal(int exitVal) {
			this.exitVal = exitVal;
		}

		public void setSuccessWrite(long successWrite) {
			this.successWrite = successWrite;
		}

		public void setFailWrite(long failWrite) {
			this.failWrite = failWrite;
		}

		public String getPLCReturnCode() {
			return plcReturnCode;
		}

		public int getExitVal() {
			return exitVal;
		}

		public long getSuccessWrite() {
			return successWrite;
		}

		public long getFailWrite() {
			return failWrite;
		}
	}

	public static class FindOutputResult extends PLCResult {
		private final boolean findValue;
		private final String lastLine;

		public FindOutputResult(int exitVal, String plcReturnCode, boolean findValue, String lastLine) {
			super(exitVal, plcReturnCode, lastLine);
			this.findValue = findValue;
			this.lastLine = lastLine;
		}

		public boolean isValueFound() {
			return findValue;
		}

		public String getLastLine() {
			return lastLine == null ? null : lastLine.substring(0,
					(lastLine.length() < LAST_LINE_MAX_LENGTH ? lastLine.length() : LAST_LINE_MAX_LENGTH));
		}
	}

	public static class HdfsFileDescriptor {
		private String fileName;
		private long fileSize;

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public void setFileSize(long fileSize) {
			this.fileSize = fileSize;
		}

		public String getFileName() {
			return fileName;
		}

		public long getFileSize() {
			return fileSize;
		}
	}

	public static class PLCResult {
		private final String plcReturnCode;
		private final int exitVal;
		private final String lastLine;

		public PLCResult(int exitVal, String plcReturnCode, String lastLine) {
			this.plcReturnCode = plcReturnCode;
			this.exitVal = exitVal;
			this.lastLine = lastLine;
		}

		public String getPLCReturnCode() {
			return plcReturnCode;
		}

		public int getExitVal() {
			return exitVal;
		}

		public String getLastLine() {
			return lastLine.substring(0, (lastLine.length() <= LAST_LINE_MAX_LENGTH ? lastLine.length()
					: LAST_LINE_MAX_LENGTH));
		}
	}

	public static class ExplainSQLResult extends PLCResult {
		private final ArrayList<String> columnNames;

		public ExplainSQLResult(int exitVal, String plcReturnCode, String lastLine, ArrayList<String> columnNames) {
			super(exitVal, plcReturnCode, lastLine);
			this.columnNames = columnNames;
		}

		public ArrayList<String> getColumnNames() {
			return columnNames;
		}
	}

	public static class HadoopResult {
		private final String hadoopReturnCode;
		private final int exitVal;

		public HadoopResult(int exitVal, String hadoopReturnCode) {
			this.exitVal = exitVal;
			this.hadoopReturnCode = hadoopReturnCode;
		}

		public int getExitVal() {
			return exitVal;
		}

		public String getHadoopReturnCode() {
			return hadoopReturnCode;
		}
	}

	public static class ListHdfsDirResult extends HadoopResult {
		private final ArrayList<HdfsFileDescriptor> descriptors;

		public ListHdfsDirResult(int exitVal, String hadoopReturnCode, ArrayList<HdfsFileDescriptor> descriptors) {
			super(exitVal, hadoopReturnCode);
			this.descriptors = descriptors;
		}

		public ArrayList<HdfsFileDescriptor> getDescriptors() {
			return descriptors;
		}
	}

	public static class CatFileResult extends HadoopResult {
		private final ArrayList<String> contents;

		public CatFileResult(int exitVal, String hadoopReturnCode, ArrayList<String> contents) {
			super(exitVal, hadoopReturnCode);
			this.contents = contents;
		}

		public ArrayList<String> getContents() {
			return contents;
		}
	}

	public static class HdfsIsEmptyFileResult extends HadoopResult {
		private final boolean isEmptyFile;
		private final String errorDesc;

		public HdfsIsEmptyFileResult(int exitVal, String hadoopReturnCode, String errorDesc, boolean isEmptyFile) {
			super(exitVal, hadoopReturnCode);
			this.isEmptyFile = isEmptyFile;
			this.errorDesc = errorDesc;
		}

		public boolean isEmptyFiles() {
			return isEmptyFile;
		}

		public String getErrorDesc() {
			return errorDesc.substring(0, (errorDesc.length() <= LAST_LINE_MAX_LENGTH ? errorDesc.length()
					: LAST_LINE_MAX_LENGTH));
		}
	}

	public static class HdfsDirExistResult extends HadoopResult {
		private final boolean dirExists;

		public HdfsDirExistResult(int exitVal, String hadoopErrorCode, boolean dirExists) {
			super(exitVal, hadoopErrorCode);
			this.dirExists = dirExists;
		}

		public boolean isDirExists() {
			return dirExists;
		}
	}

	public static class HdfsDirFileCounter extends HadoopResult {
		private final int fileCounter;

		public HdfsDirFileCounter(int exitVal, String hadoopErrorCode, int fileCounter) {
			super(exitVal, hadoopErrorCode);
			this.fileCounter = fileCounter;
		}

		public int getFileCount() {
			return fileCounter;
		}
	}
	
	
	/******************************************************************************/
	public static HdfsDirExistResult isHdfsDirExist4h2h(String hadoopCommand, 
													    LServer hdfsServer, 
													    String hdfsPath,
													    AbstractTaskRunner runner) 
		throws Exception {
		
		String host = getHostWithProtocol(hdfsServer.getVersion(), hdfsServer.getHost());
		String fsDefaultName = host + ":" + hdfsServer.getPort();

		String[] cmdArray = {
				hadoopCommand,
				"fs",
				"-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName()+ "," + 
						hdfsServer.getUserGroup(), 
				"-ls", 
				hdfsPath };

		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HdfsDirExistResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()), false);
		}

		return new HdfsDirExistResult(0, HADOOP_SUCCESS_CODE, true);
	}
	
	public static HadoopResult deleteHdfsFile4h2h(String hadoopCommand, 
												  LServer hdfsServer,
												  String hdfsPath, 
												  String pattern, 
												  AbstractTaskRunner runner) throws Exception {
		runner.writeLocalLog(Level.WARNING,
				"Check multiple hadoop client: " + hadoopCommand);

		HdfsDirExistResult result = isHdfsDirExist4h2h(hadoopCommand, hdfsServer, hdfsPath, runner);
		if (!result.isDirExists()) {
			return new HadoopResult(0, HADOOP_SUCCESS_CODE);
		}

		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		String fsDefaultName = host + ":" + hdfsServer.getPort();

		String[] cmdArray = {
				hadoopCommand,
				"fs",
				"-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(),
				"-rm",
				pattern == null ? hdfsPath : hdfsPath + File.separator + pattern
		};

		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), "3000017");
		}

		return new HadoopResult(0, HADOOP_SUCCESS_CODE);
	}
	
	public static class FindOutputResultWithSubPrition extends PLCResult {
		private final boolean findPriPartition;
		private final boolean findSubPartition;
		private final String lastLine;

		public FindOutputResultWithSubPrition(int exitVal, 
											  String plcReturnCode, 
											  boolean findPriPartition,
											  boolean findSubPartition, 
											  String lastLine) {
			super(exitVal, plcReturnCode, lastLine);
			this.findPriPartition = findPriPartition;
			this.findSubPartition = findSubPartition;
			this.lastLine = lastLine;
		}

		public boolean isSubPartitionFound() {
			return this.findSubPartition;
		}

		public boolean isPriPartitionFound() {
			return this.findPriPartition;
		}

		public String getLastLine() {
			return this.lastLine.substring(0, this.lastLine.length() < 3000 ? this.lastLine.length() : 3000);
		}
	}
	
	public static HadoopResult mergeCheckFileList(String hadoopCommand,
			                                String checkFilePathPattern,
			                                String[] subInterfaceList,
			                                Date runDate, 
			                                LServer hdfsServer,
			                                String localFilePathStr,
			                                String hdfsPath,
			                                String newCheckFileName,
			                                AbstractTaskRunner runner) 
 throws Exception {
		
		String genCheckFilePath = localFilePathStr + newCheckFileName;
		FileWriter outputFile = null;

		ArrayList<String> checkPathList = new ArrayList<String>();
		for (String subStr : subInterfaceList) {
			checkPathList.add(checkFilePathPattern.replaceAll("\\$\\{#1\\}", subStr));
		}
		
		runner.writeLocalLog(Level.INFO, "work temp path : " + localFilePathStr);
		
		File localFile = new File(localFilePathStr);

		if (!localFile.exists()) {
			
			runner.writeLocalLog(Level.INFO, "1");
			localFile.mkdirs();
		} else {
			String[] oldFileList = localFile.list();
			if(oldFileList.length > 0 ){
				for(String oldFileName : oldFileList){
					File oldFile = new File(localFilePathStr + File.separator + oldFileName);
					oldFile.delete();
				}
			}
		}
		
		try {
			File genCheckFile = new File(genCheckFilePath);
			if (genCheckFile.exists()) {
				genCheckFile.delete();
			}
						
			runner.writeLocalLog(Level.INFO, "merge check File path : " + genCheckFilePath);
			genCheckFile.createNewFile();
			outputFile = new FileWriter(genCheckFile);

			String line = null;
			String checkFileName = null;
			String checkFilePath = null;
			for (String tmpPath : checkPathList) {

				runner.writeLocalLog(Level.INFO, "begin merge check file : " + tmpPath);

				checkFileName = tmpPath.substring(tmpPath.lastIndexOf("/") + 1);
				checkFilePath = tmpPath.substring(0, tmpPath.lastIndexOf("/"));
				
				File hdfsCheckFile = new File(localFilePathStr + File.separator + checkFileName);
				if(hdfsCheckFile.exists()){
					hdfsCheckFile.delete();
				}
				
				HadoopResult result = copyActualHdfsFileToLinux(hadoopCommand, hdfsServer, checkFilePath, checkFileName,
						localFile, runner);
				if (result.getExitVal() != 0) {
					// failed return
					// and return failed message
					return result;
				}

				FileInputStream fis = null;
				InputStreamReader fread = null;
				BufferedReader reader = null;
				try {
					File tmpFile = new File(localFilePathStr + File.separator + checkFileName);
					fis = new FileInputStream(tmpFile);
					fread = new InputStreamReader(fis);
					reader = new BufferedReader(fread);
					while ((line = reader.readLine()) != null) {
						outputFile.append(line + "\n");
						runner.writeLocalLog(Level.INFO, "get info : " + line);
					}
					
					outputFile.flush();
				}
				finally {
					if(reader != null)
						reader.close();
					if(fread != null)
						fread.close();
					if(fis != null)
						fis.close();
				}
			}
			
			runner.writeLocalLog(Level.INFO, "merge check file exist boolean : " + genCheckFile.exists());
			
		}
		finally {
			if (outputFile != null)
				outputFile.close();
		}
		
		HadoopResult mkdirResult = removeOldPathAndCreateNewPath(hadoopCommand, hdfsServer, hdfsPath, newCheckFileName, runner);
		if(mkdirResult.getExitVal() != 0 ){
			return mkdirResult;
		}
		
		HadoopResult putResult = writeLinuxFileToHdfs(hadoopCommand, 
													  hdfsServer, 
													  hdfsPath, 
													  localFilePathStr, 
													  newCheckFileName,
													  runner);

		return putResult;
	}
	
	/* Copy HDFS files in a HDFS path to Linux. */
	public static HadoopResult copyActualHdfsFileToLinux(String hadoopCommand, LServer hdfsServer, String hdfsFilePath,
			String hdfsFileName, File localPath, AbstractTaskRunner runner) throws Exception {

		/* Do the actual copy. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-get",
				hdfsFilePath + File.separator + hdfsFileName, localPath.getAbsolutePath() + File.separator };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()));
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}
	
	public static HadoopResult removeOldPathAndCreateNewPath(String hadoopCommand, LServer hdfsServer, String hdfsPath,
			String fileName, AbstractTaskRunner runner) throws Exception {

		/* Initiate the variables. */
		String host = hdfsServer.getHost();
		if (!host.startsWith("hdfs://")) {
			host = "hdfs://" + host;
		}
		final String fsDefaultName = host + ":" + hdfsServer.getPort();

		/* Delete the old temporary path first to clean the old data. */
		HdfsDirExistResult result = isHdfsDirExist(hadoopCommand, hdfsServer, hdfsPath, runner);
		if (result.isDirExists()) {
			final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
					"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-rm",
					hdfsPath + File.separator + fileName };
			logCommand(cmdArray, runner);

			DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
			process.startProcess();
			process.waitAndDestroyProcess();

			if (process.getExitVal() != 0) {
				return new HadoopResult(process.getExitVal(), DELETE_HDFS_FILE_ERROR_CODE);
			}

			return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
		}

		/* Make the new directory. */
		final String[] cmdArray = new String[] { hadoopCommand, "fs", "-Dfs.default.name=" + fsDefaultName,
				"-Dhadoop.job.ugi=" + hdfsServer.getUserName() + "," + hdfsServer.getUserGroup(), "-mkdir", hdfsPath };
		logCommand(cmdArray, runner);

		DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner);
		process.startProcess();
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			return new HadoopResult(process.getExitVal(), getHadoopErrorCode(process.getLastLine()));
		}

		return new HadoopResult(SUCCESS_CODE, HADOOP_SUCCESS_CODE);
	}
	
	public static FindOutputResultWithSubPrition 
					checkPartitionWithSubPartition(String plcProgram, 
												   LServer hiveServer,
												   String dbName, 
												   String tableName, 
												   String partitionName, 
												   String subPartitionName, 
												   AbstractTaskRunner runner,
												   String plcParameter)
	throws Exception {
		String useDBCommands ="set usp.param=" + plcParameter + "; use " + dbName;
		String showPartitionCommands = "show partitions " + tableName;

		String[] cmdArray = { 
				plcProgram, 
				hiveServer.getUserName(), 
				hiveServer.getPassword(), 
				hiveServer.getHost(),
				hiveServer.getPort() + " ",
				useDBCommands + ";" + showPartitionCommands
		};

		logPLCCommand(cmdArray, runner);

		RunTaskProcess process = new RunTaskProcess(cmdArray, runner, true);
		process.waitAndDestroyProcess();

		ArrayList<String> lines = process.getAllLines();
		String lastLine = process.getNormalLastLine();
		boolean findPriPartition = false;
		boolean findSubPritition = false;
		boolean bStarted = false;
		if ((lastLine.startsWith("TDW-00000")) && (process.getExitVal() == 0)) {
			for (String line : lines) {
				if (line.equalsIgnoreCase(showPartitionCommands)) {
					bStarted = true;
				}
				if ((bStarted) && (line.equalsIgnoreCase(partitionName))) {
					findPriPartition = true;
				}
				if ((bStarted) && (findPriPartition) && (line.equalsIgnoreCase(subPartitionName))) {
					findSubPritition = true;
				}
			}
		}

		return new FindOutputResultWithSubPrition(process.getExitVal(), getPLCReturnCode(lastLine), findPriPartition,
				findSubPritition, lastLine);
	}
	
	public static PLCResult createSubPartition(String plcProgram, LServer hiveServer, String dbName, String tableName,
			String partitionName, AbstractTaskRunner runner,String plcParameter) throws Exception {
		String specialParam="SET usp.param=" + plcParameter + ";";
		String addPartitionCommand = new StringBuilder().append(specialParam).append("USE ").append(dbName).append(";")
				.append("ALTER TABLE ").append(tableName).append(" ADD SUBPARTITION ").append(partitionName)
				.append(" VALUES IN (").append(partitionName.substring(2, partitionName.length())).append(")")
				.toString();

		runner.writeLocalLog(Level.INFO, addPartitionCommand);

		String[] cmdArray = { plcProgram, 
				hiveServer.getUserName(), 
				hiveServer.getPassword(), 
				hiveServer.getHost(),
				String.valueOf(hiveServer.getPort()), 
				addPartitionCommand };

		return runProcess(cmdArray, runner);
	}
	
	public static String formatStrDateParam( String script, Date curDate ){
		
		if(script == null){
			return script;
		}
		
		int TimeOff = Calendar.YEAR;
		StringBuffer sb = new StringBuffer();
		int offset = 0;
		String value = null;
		String replaceStr = null;
		Calendar calender = Calendar.getInstance();
		boolean bFind = true;
		
		Matcher matchRes = stringPat.matcher(script);
		while (matchRes.find()) {
			String foundParam = matchRes.group();
			sb.delete(0, sb.length());
			TimeOff = Calendar.YEAR;
			bFind = true;
			calender.setTime(curDate);
			
			if (foundParam.contains("-")) {
				String[] tmpStrs = foundParam.split("-");
				sb.append(tmpStrs[1]);
				if (tmpStrs[1].endsWith("M}") || tmpStrs[1].endsWith("D}")
						|| tmpStrs[1].endsWith("H}") || tmpStrs[1].endsWith("F}")
						|| tmpStrs[1].endsWith("S}")) {
					TimeOff = DATEFORMAT.get(sb.substring(sb.length() - 2,
							sb.length() - 1));
					offset = -Integer.parseInt(sb.substring(0, sb.length() - 2)
							.trim());
				} else
					offset = -Integer.parseInt(sb.substring(0, sb.length() - 1)
							.trim());
				if(tmpStrs[0].contains("${YYYYMMDDHHFFSS")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.SECOND, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 14);
				} else if(tmpStrs[0].contains("${YYYYMMDDHHFF")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MINUTE, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 12);
				} else if (tmpStrs[0].contains("${YYYYMMDDHH")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.HOUR, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 10);
				} else if (tmpStrs[0].contains("${YYYYMMDD")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.DAY_OF_MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 8);
				} else if (tmpStrs[0].contains("${YYYYMM")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 6);
				} else if (tmpStrs[0].contains("${MM")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(4, 6);
				} else if (tmpStrs[0].contains("${DD")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.DAY_OF_MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(6, 8);
				} else if (tmpStrs[0].contains("${HH")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.HOUR, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(8, 10);
				} else if(tmpStrs[0].contains("${FF")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MINUTE, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(10, 12);
				}  else if(tmpStrs[0].contains("${SS")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.SECOND, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(12, 14);
				} else {
					bFind = false;
				}
			} else if (foundParam.contains("+")) {

				String[] tmpStrs = foundParam.split("\\+");
				sb.append(tmpStrs[1]);
				if (tmpStrs[1].endsWith("M}") || tmpStrs[1].endsWith("D}")
						|| tmpStrs[1].endsWith("H}") || tmpStrs[1].endsWith("FF}")
						|| tmpStrs[1].endsWith("S}")) {
					TimeOff = DATEFORMAT.get(sb.substring(sb.length() - 2,
							sb.length() - 1));
					offset = Integer.parseInt(sb.substring(0, sb.length() - 2)
							.trim());
				} else
					offset = Integer.parseInt(sb.substring(0, sb.length() - 1)
							.trim());
				if(tmpStrs[0].contains("${YYYYMMDDHHFFSS")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.SECOND, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 14);
				} else if(tmpStrs[0].contains("${YYYYMMDDHHFF")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MINUTE, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 12);
				} else if (tmpStrs[0].contains("${YYYYMMDDHH")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.HOUR, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 10);
				} else if (tmpStrs[0].contains("${YYYYMMDD")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.DAY_OF_MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 8);
				} else if (tmpStrs[0].contains("${YYYYMM")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(0, 6);
				} else if (tmpStrs[0].contains("${MM")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(4, 6);
				} else if (tmpStrs[0].contains("${DD")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.DAY_OF_MONTH, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(6, 8);
				} else if (tmpStrs[0].contains("${HH")) {
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.HOUR, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(8, 10);
				} else if(tmpStrs[0].contains("${FF")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.MINUTE, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(10, 12);
				}  else if(tmpStrs[0].contains("${SS")){
					if (TimeOff == Calendar.YEAR)
						calender.add(Calendar.SECOND, offset);
					else
						calender.add(TimeOff, offset);
					value = sdf.format(calender.getTime());
					replaceStr = value.substring(12, 14);
				} else {
					bFind = false;
				}
			
			} else {
				
				value = sdf.format(curDate);

				if (foundParam.contains("${YYYYMMDDHHFFSS}")) {
					replaceStr = value.substring(0, 14);
				} else if(foundParam.contains("${YYYYMMDDHHFF}")){
					replaceStr = value.substring(0, 12);
				} else if (foundParam.contains("${YYYYMMDDHH}")) {
					replaceStr = value.substring(0, 10);
				} else if (foundParam.contains("${YYYYMMDD}")) {
					replaceStr = value.substring(0, 8);
				} else if (foundParam.contains("${YYYYMM}")) {
					replaceStr = value.substring(0, 6);
				} else if (foundParam.contains("${YYYY}")){
					replaceStr = value.substring(0, 4);
				} else if (foundParam.contains("${MM}")) {
					replaceStr = value.substring(4, 6);
				} else if (foundParam.contains("${DD}")) {
					replaceStr = value.substring(6, 8);
				} else if (foundParam.contains("${HH}")) {
					replaceStr = value.substring(8, 10);
				} else if (foundParam.contains("${FF}")) {
					replaceStr = value.substring(10, 12);
				} else if (foundParam.contains("${SS}")) {
					replaceStr = value.substring(12, 14);
				} else {
					bFind = false;
				}
			
			}
			
			if (bFind) {
				script = script.replace(foundParam, replaceStr);
			}
		}
		
		return script;
	}
}
