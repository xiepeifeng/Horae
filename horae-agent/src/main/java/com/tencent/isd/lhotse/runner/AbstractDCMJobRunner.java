package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.util.CmdRunProcess;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractDCMJobRunner extends AbstractTaskRunner {

	protected final String GLOBAL_HADOOP_PATH = "hadoopPath";
	protected final String GLOBAL_JOBID_PATTERN = "jobIdPattern";
	protected final String GLOBAL_WORK_DIR = "workDir";
	protected final String GLOBAL_SCRIPT_DIR = "scriptDir";
	protected final String GLOBAL_DEFAULT_PIPE_PATH = "defaultPipePath";
	protected final String GLOBAL_PIPE_JOB_UGI = "pipeJobUGI";
	protected final int THREAD_INTERRUPT_EXIT_CODE = 10000;
	protected final int THREAD_IOEXCEPTION_EXIT_CODE = 10001;
	protected final int THREAD_OTHER_EXCEPTION_EXIT_CODE = 10002;
	private static String STR_PATTERN = "\\$\\{(YYYY)?(MM)?(DD)?(HH)?[\\-\\+]*[0-9]*[MDH]?\\}";
	private static String STR_DAYS_OF_MONTH = "\\$\\{DAYS_OF_MONTH\\(YYYYMM\\)\\}";
	String mrTaskType = "";
	Map<String, String> keyValues = new HashMap<String, String>();
	protected JobInstance job = null;
	protected String fCheck_script = null;
	protected String fPost_script = null;
	protected String outPutStr = null;
	private Pattern stringPat = null;
	private Pattern Dayofmonth = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	//this 

	private int exceptCode = 0;
	protected static final HashMap<Integer, String> ERRORCODE = new HashMap<Integer, String>() {
		private static final long serialVersionUID = 7823607212973454402L;
		{
			put(0, "[MRERROR-0] Unknown Exception");
			put(1, "[MRERROR-1] init : UnsupportedEncodingException");
			put(2, "[MRERROR-1] init : IOException");
			put(3, "[MRERROR-2] constructJobEnv : IOException");
			put(4, "[MRERROR-4] postDone : CMD Exception");
			put(5, "[MRERROR-5] tryCleanOutPut : CMD Exception");
			put(6, "[MRERROR-6] MapReduceCmdRunner : CMD Exception");
			put(7, "[MRERROR-7] MapReduceConfRunner : CMD Exception");
			put(8, "[MRERROR-8] MapReducePipeRunner : CMD Exception");
			put(9, "[MRERROR-9] HadoopCmdRun : Task Running OverTime");
		}
	};

	protected static final HashMap<String, Integer> DATEFORMAT = new HashMap<String, Integer>() {
		private static final long serialVersionUID = 587L;
		{
			put("M", Calendar.MONTH);
			put("D", Calendar.DAY_OF_MONTH);
			put("H", Calendar.HOUR);
		}
	};

	protected void commitJsonResult(Map<String, String> keyValues,
			boolean success, String runtimeId) throws IOException {

		try {
			JSONObject jsonObject = new JSONObject();
			for (Map.Entry<String, String> keyValue : keyValues.entrySet()) {
				jsonObject.put(keyValue.getKey(), keyValue.getValue());
			}

			this.writeLocalLog((success ? Level.INFO : Level.SEVERE),
					jsonObject.toString());
			this.commitTask((success ? LState.SUCCESSFUL : LState.FAILED),
					runtimeId, jsonObject.toString());
		} catch (JSONException e) {
			/* Do nothing. */
		}
	}

	protected void setexceptCode(int exceptionCode) {
		if (exceptCode > 0)
			exceptCode = exceptionCode;
	}

	protected String getErrorCodeStr() {
		if (ERRORCODE.containsKey(exceptCode))
			return ERRORCODE.get(exceptCode);
		else
			return ERRORCODE.get(0);
	}

	class JobInstance {
		int TaskType;
		String taskId;
		String curRunDateStr;
		String jobName = null;
		String taskConfArgs = null;
		String taskMainArgs = null;
		String workDir = null;
		String scriptDir = null;
		String hdfsStr = null;
		String hadoopUGI = null;
		String jobTrackerStr = null;
		String runTimeId = null;
		String hadoopCmdPrefix = null;
		String hadoopClient = null;
		int checkTimeOut = 0;
		int mainTimeOut = 0;
		String preCheckScript = null;
		String postCheckScript = null;
		String jobIdPattern = null;

		public int getCheckTimeOut() {
			return checkTimeOut;
		}

		public void setCheckTimeOut(int checkTimeOut) {
			this.checkTimeOut = checkTimeOut;
		}

		public int getMainTimeOut() {
			return mainTimeOut;
		}

		public void setMainTimeOut(int mainTimeOut) {
			this.mainTimeOut = mainTimeOut;
		}

		public String getHadoopClient() {
			return hadoopClient;
		}

		public void setHadoopClient(String hadoopClient) {
			this.hadoopClient = hadoopClient;
		}

		public String getJobName() {
			return jobName;
		}

		public void setJobName(String jobName) {
			this.jobName = jobName;
		}

		public String getWorkDir() {
			return workDir;
		}

		public void setWorkDir(String workDir) {
			this.workDir = workDir;
		}

		public String getScriptDir() {
			return scriptDir;
		}

		public void setScriptDir(String scriptDir) {
			this.scriptDir = scriptDir;
		}

		public String getHadoopJobUGI() {
			return hadoopJobUGI;
		}

		public void setHadoopJobUGI(String hadoopJobUGI) {
			this.hadoopJobUGI = hadoopJobUGI;
		}

		String nameNode = null;
		String jobTracker = null;
		String hadoopJobUGI = null;

		public int getTaskType() {
			return TaskType;
		}

		public void setTaskType(int taskType) {
			TaskType = taskType;
		}

		public String getNameNode() {
			return nameNode;
		}

		public void setNameNode(String nameNode) {
			this.nameNode = nameNode;
		}

		public String getJobTracker() {
			return jobTracker;
		}

		public void setJobTracker(String jobTracker) {
			this.jobTracker = jobTracker;
		}

		public String getTaskConfArgs() {
			return taskConfArgs;
		}

		public void setTaskConfArgs(String taskConfArgs) {
			this.taskConfArgs = taskConfArgs;
		}

		public String getTaskMainArgs() {
			return taskMainArgs;
		}

		public void setTaskMainArgs(String taskMainArgs) {
			this.taskMainArgs = taskMainArgs;
		}

		public String getPreCheckScript() {
			return preCheckScript;
		}

		public void setPreCheckScript(String preCheckScript) {
			this.preCheckScript = preCheckScript;
		}

		public String getPostCheckScript() {
			return postCheckScript;
		}

		public void setPostCheckScript(String postCheckScript) {
			this.postCheckScript = postCheckScript;
		}

		public Map<String, String> getConfPropMap() {
			return confPropMap;
		}

		public void setConfPropMap(Map<String, String> confPropMap) {
			this.confPropMap = confPropMap;
		}

		Map<String, String> confPropMap = new HashMap<String, String>();

		public JobInstance(LTask task) throws IOException {
			try {
				this.taskId = task.getId();
				// this.taskName = task.get
				long dateMicroSeconds = task.getCurRunDate();
				String offRange = AbstractDCMJobRunner.this
						.getExtPropValue("off_range");
				if (offRange != null && offRange.equals("0")) {
					dateMicroSeconds = task.getNextRunDate();
				}
				writeLocalLog(Level.INFO, String.format("%d", dateMicroSeconds));
				Date curRunDate = new Date(dateMicroSeconds);
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
						"yyMMddHHmmss");
				this.curRunDateStr = simpleDateFormat.format(curRunDate);
				this.runTimeId = String.format("%s_%s", this.taskId,
						this.curRunDateStr);
				try {
					this.workDir = requestGlobalParameters().get(GLOBAL_WORK_DIR)
							+ "/" + this.taskId + "/" + this.curRunDateStr;
					this.scriptDir = requestGlobalParameters().get(GLOBAL_SCRIPT_DIR)
							+ "/" + this.taskId;
				} catch (UnsupportedEncodingException e) {
					keyValues
							.put("task_desc", "Get Meta Data of MR task ERROR");
					commitJsonResult(keyValues, false, this.runTimeId);
					writeLocalLog(Level.SEVERE,
							CommonUtils.stackTraceToString(e));
					return;
				} catch (IOException e) {
					keyValues
							.put("task_desc", "Get Meta Data of MR task ERROR");
					commitJsonResult(keyValues, false, this.runTimeId);
					writeLocalLog(Level.SEVERE,
							CommonUtils.stackTraceToString(e));
					return;
				}

				this.TaskType = task.getType();
				LServer nameNodeServer = task.getSourceServers(0);
				String userName = nameNodeServer.getUserName();
				String userGroup = nameNodeServer.getUserGroup();
				this.hadoopUGI = String.format("%s,%s", userName, userGroup);
				String hdfsHost = nameNodeServer.getHost();
				int hdfsPort = nameNodeServer.getPort();
				this.hdfsStr = String
						.format("hdfs://%s:%d", hdfsHost, hdfsPort);
				LServer jobTrackerServer = task.getSourceServers(1);
				this.confPropMap.put("tdw.ugi.groupname", jobTrackerServer.getUserGroup());
				String jobTrackerHost = jobTrackerServer.getHost();
				int jobTrackerPort = jobTrackerServer.getPort();
				this.jobTrackerStr = String.format("%s:%d", jobTrackerHost,
						jobTrackerPort);
				String hadoopVersion = jobTrackerServer.getVersion();
				writeLocalLog(Level.INFO, "hadoopVersion" + hadoopVersion);
				this.hadoopClient = requestGlobalParameters().get(GLOBAL_HADOOP_PATH
						+ "_" + hadoopVersion);
				writeLocalLog(Level.INFO, hadoopClient);
				this.jobIdPattern = requestGlobalParameters().get(GLOBAL_JOBID_PATTERN + "_" + hadoopVersion);
				writeLocalLog(Level.INFO, "jobIdPattern:" + this.jobIdPattern);
				
				/*Change for HA*/
				LServer zooKeeper = null;
				if(hadoopVersion.equalsIgnoreCase("HA")) {
					zooKeeper = task.getSourceServers(2);
					this.confPropMap.put("zookeeper.address", zooKeeper.getHost());
				}
				
				this.hadoopCmdPrefix = String.format(
						"%s fs -Dfs.default.name=%s -Dhadoop.job.ugi=%s ",
						this.hadoopClient, this.hdfsStr, this.hadoopUGI);
				this.jobName = String.format("%s-%s-%s", task.getAction(),
						this.taskId, this.curRunDateStr);
				/*Change for HA*/
				//this.confPropMap.put("zookeeper.address", zooKeeper.getHost());
				
				this.confPropMap.put("mapred.job.tracker", this.jobTrackerStr);
				this.confPropMap.put("fs.default.name", this.hdfsStr);
				this.confPropMap.put("hadoop.job.ugi", this.hadoopUGI);
				this.confPropMap.put("mapred.job.name", this.jobName);
				//set usp.param arguments
				Date dt = new Date(dateMicroSeconds);
				Calendar c = Calendar.getInstance();
				c.setTime(dt);
				String tempDateStr = sdf.format(dt);
				this.confPropMap.put("usp.param", task.getId() + "_" + tempDateStr.substring(0, 10));
				// common task arguments
				String tempArgu = getExtPropValue("task.configuration.param");
				if (tempArgu != null) {
					String[] propArray = string2DateStr(tempArgu,
							dateMicroSeconds).split("\n");
					for (int propIndex = 0; propIndex < propArray.length; propIndex++) {
						String[] subPropArray = propArray[propIndex].split("=");
						confPropMap.put(subPropArray[0].trim(),
								subPropArray[1].trim());
					}
				}
				tempArgu = getExtPropValue("pre.check.script");
				if (tempArgu != null) {
						setPreCheckScript(string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				tempArgu = getExtPropValue("post.check.script");
				if (tempArgu != null) {
					setPostCheckScript(string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				tempArgu = getExtPropValue("task.check.timeout");
				if (tempArgu != null) {
					this.setCheckTimeOut(Integer.valueOf(tempArgu));
				}
				tempArgu = getExtPropValue("task.main.timeout");
				if (tempArgu != null) {
					this.setMainTimeOut(Integer.valueOf(tempArgu));
				}
				// confRunners arguments
				tempArgu = getExtPropValue("mapred.jar");
				if (tempArgu != null) {
					confPropMap.put("mapred.jar",
							string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				tempArgu = getExtPropValue("mapred.input.dir");
				if (tempArgu != null) {
					confPropMap.put("mapred.input.dir",
							string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				tempArgu = getExtPropValue("mapred.output.dir");
				if (tempArgu != null) {
					confPropMap.put("mapred.output.dir",
							string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				tempArgu = getExtPropValue("mapred.mapper.class");
				if (tempArgu != null) {
					confPropMap.put("mapred.mapper.class", tempArgu.trim());
				}
				tempArgu = getExtPropValue("mapred.reducer.class");
				if (tempArgu != null) {
					confPropMap.put("mapred.reducer.class", tempArgu.trim());
				}
				tempArgu = getExtPropValue("mapred.output.key.class");
				if (tempArgu != null) {
					confPropMap.put("mapred.output.key.class", tempArgu.trim());
				}
				tempArgu = getExtPropValue("mapred.output.value.class");
				if (tempArgu != null) {
					confPropMap.put("mapred.output.value.class",
							tempArgu.trim());
				}

				// cmdRunner argument
				tempArgu = getExtPropValue("task.main.param");
				if (tempArgu != null) {
					setTaskMainArgs(string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				// pipesRunner argument
				tempArgu = getExtPropValue("hadoop.pipes.executable");
				if (tempArgu != null) {
					confPropMap.put("hadoop.pipes.executable",
							string2DateStr(tempArgu.trim(), dateMicroSeconds));
				}
				tempArgu = getExtPropValue("hadoop.pipes.java.recordreader");
				if (tempArgu != null) {
					confPropMap.put("hadoop.pipes.java.recordreader",
							tempArgu.trim());
				}
				tempArgu = getExtPropValue("hadoop.pipes.java.recordwriter");
				if (tempArgu != null) {
					confPropMap.put("hadoop.pipes.java.recordwriter",
							tempArgu.trim());
				}
				
				//read env.conf
				//readUserConf( confPropMap );
			} catch (Exception e) {
				writeExceptionTraceToLog(e);
				throw new IOException(e);
			}
		}
		
		void readUserConf() {
			String confEnv = this.getWorkDir() + "/env.conf";
			File conf = new File(confEnv);
			BufferedReader br = null;
			FileInputStream fis = null;
			try {
				if (conf.exists()) {
					fis = new FileInputStream(confEnv);
					br = new BufferedReader(new InputStreamReader(fis));
					String data = "";
					while ((data = br.readLine()) != null) {
						String[] argu = data.split("=");
						if (argu.length > 1) {
							confPropMap.put(argu[0].trim(), argu[1].trim());
						}
					}
				}
			} catch (Exception e) {
				writeExceptionTraceToLog(e);
			} finally {
				try {
					if( br != null  )
					{
						br.close();
					}
					if( fis != null )
					{
						fis.close();
					}
				} catch (Exception ee) {
					writeExceptionTraceToLog(ee);
				}
			}
		}

		public String getRunTimeId() {
			return runTimeId;
		}

		public void setRunTimeId(String runTimeId) {
			this.runTimeId = runTimeId;
		}

		public String getHadoopCmdPrefix() {
			return hadoopCmdPrefix;
		}

		public void setHadoopCmdPrefix(String hadoopCmdPrefix) {
			this.hadoopCmdPrefix = hadoopCmdPrefix;
		}

		public String getTaskId() {
			return taskId;
		}

		public void setTaskId(String taskId) {
			this.taskId = taskId;
		}

		public String getCurRunDateStr() {
			return curRunDateStr;
		}

		public void setCurRunDateStr(String curRunDateStr) {
			this.curRunDateStr = curRunDateStr;
		}

		public String getHdfsStr() {
			return hdfsStr;
		}

		public void setHdfsStr(String hdfsStr) {
			this.hdfsStr = hdfsStr;
		}

		public String getHadoopUGI() {
			return hadoopUGI;
		}

		public void setHadoopUGI(String hadoopUGI) {
			this.hadoopUGI = hadoopUGI;
		}

		public String getJobTrackerStr() {
			return jobTrackerStr;
		}

		public void setJobTrackerStr(String jobTrackerStr) {
			this.jobTrackerStr = jobTrackerStr;
		}
		
		public String getJobIdPattern(){
			return jobIdPattern;
		} 
	}

	public AbstractDCMJobRunner() {
		// init();
		stringPat = Pattern.compile(STR_PATTERN);
		Dayofmonth = Pattern.compile(STR_DAYS_OF_MONTH);
	}

	protected void init(LTask task) throws UnsupportedEncodingException,
			IOException {
		this.writeLocalLog(Level.INFO, task.getId());
		try {
			this.job = new JobInstance(task);
			this.writeLocalLog(Level.INFO, "get Job");
		} catch (UnsupportedEncodingException e) {
			setexceptCode(1);
			throw e;
		} catch (IOException e) {
			setexceptCode(2);
			throw e;
		}
	}

	/**
	 * replace the $[YYYYMMDD] string This function for the check operation in
	 * TDCP
	 * 
	 * @param
	 * @return
	 * @throws IOException
	 */
	protected boolean constructJobEnv() throws Exception {
		boolean flag = true;
		File workDir = new File(this.job.getWorkDir());
		File scriptDir = new File(this.job.getScriptDir());
		try {
			if (!workDir.exists()) {
				workDir.mkdirs();
			}
			if (scriptDir.exists()) {
				HashSet<String> workDirSet = new HashSet<String>();
				String[] workDirArray = workDir.list();
				for (int ind = 0; ind < workDirArray.length; ind++)
					workDirSet.add(workDirArray[ind]);
				String[] scriptDirArray = scriptDir.list();
				for (int ind = 0; ind < scriptDirArray.length; ind++) {
					String scriptItem = scriptDirArray[ind];
					if (!workDirSet.contains(scriptItem)) {
						String mkSoftLink = String.format("ln -s %s %s",
								this.job.getScriptDir() + "/" + scriptItem,
								this.job.getWorkDir() + "/" + scriptItem);
						this.writeLocalLog(Level.INFO, "[MAKE SOFT LINK]"
								+ mkSoftLink);
						flag = this.runCmd(mkSoftLink, 5);
					}
				}
			}
			this.job.readUserConf();
		} catch (Exception e) {
			setexceptCode(3);
			throw e;
		}
		return flag;
	}

	protected boolean preCheck() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, this.job.getCheckTimeOut());
		StringBuffer cmdStr = new StringBuffer();
		boolean flag = true;
		if (this.job.getPreCheckScript() == null)
			return flag;
		fCheck_script = this.job.getPreCheckScript().trim();
		String[] cmdStrs = fCheck_script.split(";");
		if (cmdStrs.length <= 0) {
			return flag;
		}
		while (Calendar.getInstance().compareTo(cal) < 0) {
			try {
				flag = true;
				for (int i = 0; flag && (i < cmdStrs.length); i++) {
					cmdStr.delete(0, cmdStr.length());
					String cmdArgs[] = cmdStrs[i].trim().split(" +");
					if (cmdArgs[0].trim().equalsIgnoreCase("check")) {
						// ls, check the file exist!
						cmdStr.append(this.job.getHadoopCmdPrefix());
						cmdStr.append("-ls " + cmdArgs[1]);
					} else {
						// shell command check!
						if (cmdArgs[0].trim().toLowerCase().indexOf(".sh") != -1) {
							cmdStr.append("sh " + cmdStrs[i].trim());
						} else {
							cmdStr.append(cmdStrs[i].trim());
						}
					}
					this.writeLocalLog(Level.INFO,
							"[PRE CHECK CMD]" + cmdStr.toString());
					// this command should add time limit
					flag = runCmd(cmdStr.toString(), 5);
					if (flag == false) {
						this.writeLocalLog(Level.INFO, "[PRE CHECK CMD]"
								+ "Sleep Begin!");
						Thread.sleep(60 * 1000);
						this.writeLocalLog(Level.INFO, "[PRE CHECK CMD]"
								+ "Sleep Over!");
					}
				}
				if (flag == true)
					return true;
			} catch (Exception e) {
				try {
					writeExceptionTraceToLog(e);
					this.writeLocalLog(Level.INFO, "[PRE CHECK CMD]"
							+ "Sleep Begin!");
					Thread.sleep(60 * 1000);
					this.writeLocalLog(Level.INFO, "[PRE CHECK CMD]"
							+ "Sleep Over!");
				} catch (InterruptedException e1) {
					writeExceptionTraceToLog(e1);
					continue;
				}
				continue;
			}
		}
		return flag;
	}

	/**
	 * replace the $[YYYYMMDD] string This function for the done operation in
	 * TDCP
	 * 
	 * @param
	 * @return
	 * @throws IOException
	 */
	protected boolean postDone() throws Exception {

		boolean flag = true;
		StringBuffer cmdStr = new StringBuffer();

		if (this.job.getPostCheckScript() == null)
			return flag;

		fPost_script = this.job.getPostCheckScript().trim();
		String[] cmdStrs = fPost_script.split(";");

		if (cmdStrs.length <= 0) {
			return flag;
		}

		for (int i = 0; flag && (i < cmdStrs.length); i++) {

			cmdStr.delete(0, cmdStr.length());
			String cmdArgs[] = cmdStrs[i].trim().split(" +");
			if (cmdArgs[0].trim().equalsIgnoreCase("done")) {
				// touchz, make done file!
				cmdStr.append(this.job.getHadoopCmdPrefix());
				cmdStr.append("-touchz " + cmdArgs[1]);
			} else {
				// other command!
				if (cmdArgs[0].trim().toLowerCase().indexOf(".sh") != -1) {
					cmdStr.append("sh -x  " + cmdStrs[i].trim());
				} else {
					cmdStr.append(cmdStrs[i].trim());
				}
			}
			this.writeLocalLog(Level.INFO,
					"[POST DONE CMD]" + cmdStr.toString());
			// add time limit 3600s,return
			try {
				flag = runCmd(cmdStr.toString(), 60);
			} catch (Exception e) {
				setexceptCode(4);
				throw e;
			}
		}
		return flag;
	}

	/**
	 * clean the mapred.output.dir
	 * 
	 * @param param
	 * @return
	 * @throws IOException
	 */
	protected boolean tryCleanOutPut() throws Exception {
		if (this.job.getConfPropMap().containsKey("mapred.output.dir")) {
			outPutStr = this.job.getConfPropMap().get("mapred.output.dir");
			String lsCmdStr = this.job.getHadoopCmdPrefix() + "-ls "
					+ outPutStr;
			String cleanCmdStr = this.job.getHadoopCmdPrefix() + "-rmr "
					+ outPutStr;
			this.writeLocalLog(Level.INFO, "[PRE-LS-OUTDIR]" + lsCmdStr);
			try {
				if (runCmd(lsCmdStr, 5)) {
					this.writeLocalLog(Level.INFO, "[PRE-CLEAN-OUTDIR]"
							+ cleanCmdStr);
					if (runCmd(cleanCmdStr, 5))
						return true;
					else
						return false;
				} else
					return true;
			} catch (Exception e) {
				setexceptCode(5);
				throw e;
			}
		}
		return true;
	}

	/**
	 * run hadoop cmd for check, done, clean
	 * 
	 * @param Cmd
	 * 
	 * @return
	 * @throws IOException
	 */
	protected boolean runCmd(String cmd, int timeout) throws Exception {
		this.writeLocalLog( Level.INFO, "[CMD]" + cmd);
		CmdRunProcess cmdProcess = new CmdRunProcess(cmd, null,
				this.job.getWorkDir(), timeout, this);
		cmdProcess.startProcess();
		cmdProcess.waitAndDestroyProcess();
		if (cmdProcess.isOverTime()) {
			this.writeLocalLog(Level.WARNING,
					"exit code is " + cmdProcess.getExitCode());
			if (cmdProcess.getExitCode() == 0) {
				return true;
			}
		} else {
			this.writeLocalLog(Level.WARNING, "cmd execute overtime!");
		}
		return false;
	}

	/**
	 * run hadoop job command
	 * 
	 * @param Cmd
	 * 
	 * @return
	 * @throws IOException
	 */
	protected boolean runhadoopCmd(String cmd, int timeout) throws Exception {
		
		this.writeLocalLog( Level.INFO, "[CMD]" + cmd);
		
		String jobIdPattern = this.job.getJobIdPattern();
		if(jobIdPattern == null){
			jobIdPattern = "\\s+job_[0-9]*_[0-9]*";
		}
		Pattern stringPat = Pattern.compile(jobIdPattern);
		Matcher matchRes = null;
		CmdRunProcess cmdProcess = new CmdRunProcess(cmd, null,
				this.job.getWorkDir(), timeout, this);
		cmdProcess.startProcess();
		cmdProcess.waitAndDestroyProcess();
		if (cmdProcess.isOverTime()) {
			this.writeLocalLog(Level.WARNING,
					"exit code is " + cmdProcess.getExitCode());
			if (cmdProcess.getExitCode() == 0) {
				this.writeLocalLog(Level.INFO, "cmd execute success!");
				return true;
			}
		} else {
			this.writeLocalLog(Level.WARNING, "cmd execute overtime!");
			this.writeLocalLog(Level.WARNING,
					"exit code is " + cmdProcess.getExitCode());
			String outputStr = cmdProcess.getOutputStr();
			String errorStr = cmdProcess.getErrStr();
			this.writeLocalLog(Level.INFO,
					"Output is :" + outputStr
							+ "error out is :" + errorStr);
			matchRes = stringPat.matcher(outputStr + errorStr);
			if (matchRes.find()) {
				String jobId = matchRes.group().trim();
				this.writeLocalLog(Level.INFO, "job Id:" + jobId);
				killJob(jobId);
			}
		}
		return false;
	}

	protected void killJob(String tmpJobID) {

		String killCmd = job.getHadoopClient() + " job -Dmapred.job.tracker="
				+ this.job.getJobTrackerStr() + " -kill " + tmpJobID;
		try {
			runCmd(killCmd, 5);
		} catch (Exception e) {
			writeExceptionTraceToLog(e);
		}
	}

	public boolean toJobXml() {
		return true;
	}

	abstract boolean executeTask() throws Exception;

	public void execute() throws IOException {
		try {
			init(this.getTask());
			String logString = "Start running MrCmd:" + this.job.getTaskId()
					+ ", date:" + this.job.getCurRunDateStr();
			// comit task status
			commitTask(LState.RUNNING, "Mr" + this.mrTaskType, logString);
			this.writeLocalLog(Level.INFO, "1,Check running conditions");
			this.writeLocalLog(Level.INFO, logString);
			/*
			 * first:construct env used by job execution
			 */
			if (constructJobEnv()) {
				this.writeLocalLog(Level.INFO, "Env construction complete!");
			} else {
				keyValues.put("task_desc", "Env construction failed!");
				commitJsonResult(keyValues, false, "");
				this.writeLocalLog(Level.INFO,
						"[ERROR]Env construction failed!");
				return;
			}
			/*
			 * second:check conitions befor execution,e.g:source files
			 */
			this.writeLocalLog(Level.INFO, "2,Check running conditions");
			if (preCheck()) {
				this.writeLocalLog(Level.INFO, "Condition check OK!");
			} else {
				keyValues.put("task_desc", "Condition check failed!");
				commitJsonResult(keyValues, false, "");
				this.writeLocalLog(Level.INFO, "Condition check failed!");
				return;
			}

			/*
			 * third:clean existing target dir
			 */
			this.writeLocalLog(Level.INFO,
					"3,Clean existing target output dir!");
			if (tryCleanOutPut()) {
				this.writeLocalLog(Level.INFO, "Clean existing output dir OK!");
			} else {
				keyValues.put("task_desc", "Clean existing output dir failed!");
				commitJsonResult(keyValues, false, "");
				this.writeLocalLog(Level.INFO,
						"Clean existing output dir failed!");
				return;
			}
			/*
			 * forth:generate job conf file is neccesary
			 */
			this.writeLocalLog(Level.INFO,
					"4,Generate job configuration infomation");
			if (toJobXml()) {
				this.writeLocalLog(Level.INFO,
						"Create job configuration file OK!");
			} else {
				keyValues.put("task_desc",
						"Create job configuration file failed!");
				commitJsonResult(keyValues, false, "");
				this.writeLocalLog(Level.INFO,
						"Create job configuration file failed!");
				return;
			}
			/*
			 * fifth:execute task
			 */

			this.writeLocalLog(Level.INFO, "5,Executing task");
			if (executeTask()) {
				this.writeLocalLog(Level.INFO, String.format(
						"Mr%s runing successful finished.", this.mrTaskType));
			} else {
				keyValues.put("task_desc",
						String.format("Mr%s run failed!", this.mrTaskType));
				commitJsonResult(keyValues, false, "");
				this.writeLocalLog(Level.INFO,
						String.format("Mr%s runing failed!", this.mrTaskType));
				return;
			}
			/*
			 * sixth:touch done file in target output dir
			 */
			this.writeLocalLog(Level.INFO,
					"6,Generate done file in output dir!");
			if (postDone()) {
				keyValues.put("task_desc", "Done file generate succefully");
				commitJsonResult(keyValues, true, "");
				this.writeLocalLog(Level.INFO, String.format(
						"Mr%s MRRuning successful finished.", this.mrTaskType));
			} else {
				keyValues.put("task_desc", String.format(
						"Mr%s post Done Failed!", this.mrTaskType));
				commitJsonResult(keyValues, false, "");
				this.writeLocalLog(Level.INFO, String.format(
						"Mr%s generate done file failed!", this.mrTaskType));
				return;
			}

		} catch (Exception e) {
			this.writeExceptionTraceToLog(e);
			keyValues.put("task_desc", getErrorCodeStr());
			commitJsonResult(keyValues, false, "");
		}
	}

	/**
	 * get the exception string
	 * 
	 * param Throwable
	 * 
	 */
	protected void writeExceptionTraceToLog(Throwable e) {
		if (e != null) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			String exceptStr = null;
			try {
				e.printStackTrace(pw);
				exceptStr = sw.toString();
			} catch (Exception ex) {
				this.writeLocalLog(Level.WARNING,
						"[EXCEPTIONERROR]StringWriter close Error");
			} finally {
				try {
					if( pw != null )
					{
						pw.close();
					}
					if( sw != null )
					{
						sw.close();
					}
				} catch (Exception ex) {
					this.writeLocalLog(Level.WARNING,
							"[EXCEPTION-ERROR]printStackTrace Error");
				}
			}
			if (exceptStr != null)
				this.writeLocalLog(Level.WARNING, "[EXCEPTION]" + exceptStr);
		}
	}

	/**
	 * replace the ${YYYYMMDD -\+ [0-9]} string
	 * 
	 * @param param
	 * @return
	 * @throws IOException
	 */
	public String string2DateStr(String script, long timeStamp) {

		String[] tmpStrs = null;
		String value = null;
		String replaceStr = null;
		StringBuffer sb = new StringBuffer();
		int offset = 0;
		boolean bFind = true;
		Date dt = new Date(timeStamp);
		Calendar c = Calendar.getInstance();
		c.setTime(dt);
		value = sdf.format(dt);

		int TimeOff = Calendar.YEAR;

		Matcher matchRes = stringPat.matcher(script);
		while (matchRes.find()) {
			String foundParam = matchRes.group();
			sb.delete(0, sb.length());
			TimeOff = Calendar.YEAR;
			c.setTime(dt);
			value = sdf.format(dt);
			if (foundParam.contains("-")) {
				tmpStrs = foundParam.split("-");
				sb.append(tmpStrs[1]);
				if (tmpStrs[1].endsWith("M}") || tmpStrs[1].endsWith("D}")
						|| tmpStrs[1].endsWith("H}")) {
					TimeOff = DATEFORMAT.get(sb.substring(sb.length() - 2,
							sb.length() - 1));
					offset = -Integer.parseInt(sb.substring(0, sb.length() - 2)
							.trim());
				} else
					offset = -Integer.parseInt(sb.substring(0, sb.length() - 1)
							.trim());
				if (tmpStrs[0].contains("${YYYYMMDDHH")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.HOUR, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(0, 10);
				} else if (tmpStrs[0].contains("${YYYYMMDD")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.DAY_OF_MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(0, 8);
				} else if (tmpStrs[0].contains("${YYYYMM")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(0, 6);
				} else if (tmpStrs[0].contains("${MM")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(4, 6);
				} else if (tmpStrs[0].contains("${DD")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.DAY_OF_MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(6, 8);
				} else if (tmpStrs[0].contains("${HH")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.HOUR, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(8, 10);
				} else {
					bFind = false;
				}
			} else if (foundParam.contains("+")) {
				tmpStrs = foundParam.split("\\+");
				sb.append(tmpStrs[1]);
				if (tmpStrs[1].endsWith("M}") || tmpStrs[1].endsWith("D}")
						|| tmpStrs[1].endsWith("H}")) {
					TimeOff = DATEFORMAT.get(sb.substring(sb.length() - 2,
							sb.length() - 1));
					offset = Integer.parseInt(sb.substring(0, sb.length() - 2)
							.trim());
				} else
					offset = Integer.parseInt(sb.substring(0, sb.length() - 1)
							.trim());
				if (tmpStrs[0].contains("${YYYYMMDDHH")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.HOUR, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(0, 10);
				} else if (tmpStrs[0].contains("${YYYYMMDD")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.DAY_OF_MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(0, 8);
				} else if (tmpStrs[0].contains("${YYYYMM")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(0, 6);
				} else if (tmpStrs[0].contains("${MM")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(4, 6);
				} else if (tmpStrs[0].contains("${DD")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.DAY_OF_MONTH, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(6, 8);
				} else if (tmpStrs[0].contains("${HH")) {
					if (TimeOff == Calendar.YEAR)
						c.add(Calendar.HOUR, offset);
					else
						c.add(TimeOff, offset);
					value = sdf.format(c.getTime());
					replaceStr = value.substring(8, 10);
				} else {
					bFind = false;
				}
			} else {
				if (foundParam.contains("${YYYYMMDDHH}")) {
					replaceStr = value.substring(0, 10);
				} else if (foundParam.contains("${YYYYMMDD}")) {
					replaceStr = value.substring(0, 8);
				} else if (foundParam.contains("${YYYYMM}")) {
					replaceStr = value.substring(0, 6);
				} else if (foundParam.contains("${MM}")) {
					replaceStr = value.substring(4, 6);
				} else if (foundParam.contains("${DD}")) {
					replaceStr = value.substring(6, 8);
				} else if (foundParam.contains("${HH}")) {
					replaceStr = value.substring(8, 10);
				} else {
					bFind = false;
				}
			}
			if (bFind) {
				script = script.replace(foundParam, replaceStr);
			}
		}

		Matcher mDayofmonth = Dayofmonth.matcher(script);
		while (mDayofmonth.find()) {
			c.setTime(dt);
			c.set(Calendar.DAY_OF_MONTH, 1);
			c.roll(Calendar.DAY_OF_MONTH, -1);
			String sDayOfMonth = sdf.format(c.getTime());
			String foundParam = mDayofmonth.group();
			script = script.replace(foundParam, sDayOfMonth.substring(0, 8));
		}

		return script;
	}

}
