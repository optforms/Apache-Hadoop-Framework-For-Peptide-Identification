package com.optforms.mrexecutor.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * This mapper does nothing but writes input splits to a staging area
 * on local file system and then execute binaries of algorithms which use
 * these staged files and produce output.
 * 
 * These output files are then copied back to HDFS by this mapper in its cleanup step.
 * 
 *  setup() phase initializes required properties and objects.
 *  map() phase writes records from input split to a staging area.
 *  cleanup() step triggers required algorithms and copies back their output to HDFS. 
 *
 */
public class ExecutorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//Logs will be accessible on this app's Application master's Web UI and Job History server.
	private final static Log logger = LogFactory.getLog(ExecutorMapper.class);
	//This is the root logger provided by log4j
	Logger rootLogger = Logger.getRootLogger();

	//Points to directory on local file system where intermediate files are staged.
	private String stagingBaseDirname = "";
	//Points to directory on local file system where intermediate input files to algorithm are staged.
	private String stagingInDirname = "";
	//Points to directory on local file system where intermediate ouput files from an algorithm are staged.
	private String stagingOutDirname = "";
	//Points to file in input staging dir in which input split records are written.
	private String stagingInputFile = "";

	private String hdfsOutDir = "";

	private FileWriter stagedInputFileWriter = null;
	//This mapper's task id.
	private String myTaskId = "";
	//This mapper's attempt id.
	private String myAttemptId = "";

	private String logFile = "";

	private String dataHeader = null;
	private String algoBinHome = null;
	private String mcrRoot = null;

	/**
	 * Prepares this mapper instance for execution.
	 * 	1. Reads configuration values.
	 *  2. Opens file writer to intermediate staging input file on local file system.
	 *  3. Writes header to this intermediate staging input file if required. 
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		//Get task and attempt ids.
		myTaskId = context.getTaskAttemptID().getTaskID().toString();
		myAttemptId = context.getTaskAttemptID().toString();

		logger.info("My task id = " + myTaskId + ", my attempt id = " + myAttemptId);

		logger.info("Reading properties from configuration object");
		//Read properties from job configuration. These are set in Driver.
		hdfsOutDir = context.getConfiguration().get("OUT_DIR");
		logger.debug("hdfsOutDir=" + hdfsOutDir);
		stagingBaseDirname = context.getConfiguration().get("STAGE_DIR") + "/"
				+ myTaskId + "/" + myAttemptId;

		String logsDir = stagingBaseDirname + "/logs/";
		new File(logsDir).mkdirs();

		logFile = logsDir + myAttemptId + ".log";

		rootLogger.setLevel(Level.DEBUG);
		//Define log pattern layout
		PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");
		//Define file appender with layout and output log file name
		RollingFileAppender fileAppender = new RollingFileAppender(layout, logFile);
		//Add the appender to root logger
		rootLogger.addAppender(fileAppender);

		logger.debug("stagingBaseDirname=" + stagingBaseDirname);
		algoBinHome = context.getConfiguration().get("ALGO_BIN_HOME");
		logger.debug("algoBinHome=" + algoBinHome);
		mcrRoot = context.getConfiguration().get("MCR_ROOT");
		logger.debug("mcrRoot=" + mcrRoot);
		dataHeader = context.getConfiguration().get("DATA_HEADER");
		logger.debug("dataHeader=" + dataHeader);

		stagingInDirname = stagingBaseDirname + "/in/";
		stagingOutDirname = stagingBaseDirname + "/out/";

		//Create staging directories.
		if(!new File(stagingInDirname).mkdirs()) {
			String err = "Failed to create staging directory '" + stagingInDirname + "'. Please make sure you have sufficient read and write rights to this directory.";
			logger.fatal(err);
			throw new IOException(err);
		} else {
			logger.info("Successfully created input staging directory at '" + stagingInDirname + "'.");
		}

		if(!new File(stagingOutDirname).mkdirs()) {
			String err = "Failed to create staging directory '" + stagingOutDirname + "'. Please make sure you have sufficient read and write rights to this directory.";
			logger.fatal(err);
			throw new IOException(err);
		} else {
			logger.info("Successfully created output staging directory at '" + stagingOutDirname + "'.");
		}

		stagingInputFile = stagingInDirname + "/" + myAttemptId + ".txt";
		logger.debug("stagingInDirname=" + stagingInDirname);

		File sf = new File(stagingInputFile);
		sf.createNewFile();

		stagedInputFileWriter = new FileWriter(sf);
		logger.info("Opened staging input file writer");

		//Write header to this mapper's staged input file if required.
		if(context.getConfiguration().getBoolean("ADD_DATA_HEADER", false)) {
			logger.info("Header written to staging input file");
			stagedInputFileWriter.write(dataHeader + "\n");
		}
	}

	/**
	 * Write each record from input split to intermediate staging file on local file system
	 * Each mappper will have it's own such file. Also, if algorithm expects header in input file, then each of this file should have header too.
	 * This header can be set in properties JSON
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		stagedInputFileWriter.write(value.toString() + "\n");
	}

	/**
	 * Releases resources.
	 * Then triggers required algorithm with intermediate staged input file.
	 * And output from this algorithm is written back to HDFS.
	 *  
	 */
	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
		super.cleanup(context);
		logger.debug("Closing staged input file writer");
		stagedInputFileWriter.flush();
		stagedInputFileWriter.close();

		//Set environment variables for algorithm's shell scripts
		String[] env = new String[1];
		env[0] = "MCR_CACHE_ROOT=" + context.getConfiguration().get("MCR_CACHE_ROOT", "/tmp");

		logger.debug("MCR_CACHE_ROOT = " + env[0]);

		//Get the list of commands that are required to trigger algorith.
		String[] commands = context.getConfiguration().getStrings("COMMANDS");

		//Maintain a map of input and temporary file paths passed to algorithm scripts. 
		//This allows us to pass same paths to multiple, different scripts within same algorithm.
		Map<String, String> argFileMap = new HashMap<String, String>();
		argFileMap.put("%INPUT_FILE%", stagingInputFile);

		int tmp_counter = 0;
		for (String cmd : commands) {
			logger.debug("Found command string = " + cmd);
			//Replace standard variables from shell script arguments.
			cmd = algoBinHome + "/" + cmd.replaceAll("%MCR_ROOT%", mcrRoot);
			cmd = cmd.replaceAll("%INPUT_FILE%", stagingInputFile);

			String[] tokens = cmd.split(" ");
			for (String tok : tokens) {
				//Check if a temporary .mat file path is to be passed to current command.
				if(tok.startsWith("%TMP_MAT_FILE_")) {
					if(argFileMap.get(tok) == null) {
						argFileMap.put(tok, stagingOutDirname + "/" + myAttemptId + "_" + tmp_counter + ".mat");
						tmp_counter++;
					}
					cmd = cmd.replaceAll(tok, argFileMap.get(tok));
				}
			}
			//Trigger the execution of one script from this algorithm.
			logger.debug("Executable command string = '" + cmd + "'");
			executeSh(cmd, env);
		}

		logger.info("Copying output files to HDFS");
		//Copy output .txt files generated by algorithm scripts to HDFS output directory.
		FileSystem fs = FileSystem.newInstance(context.getConfiguration());
		File outFiles = new File(stagingOutDirname);
		File[] txtFiles = outFiles.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return (pathname.isFile() && pathname.toString().endsWith(
						".txt"));
			}
		});
		//Copy each .txt file from staged output directory on local file system to HDFS
		for (File txtFile : txtFiles) {
			Path srcPath = new Path(txtFile.toString());
			Path destPath = new Path(hdfsOutDir);
			logger.debug("Copying output file " + txtFile.toString() + " to "  + hdfsOutDir + " on HDFS");
			fs.copyFromLocalFile(srcPath, destPath);
		}
		logger.debug("Closing FS handler");
		fs.close();
	}

	/**
	 * Private utility method to trigger execution of script and read its output and error streams.
	 * 
	 * @param command	Shell command to be executed
	 * @param env	Environment variables to be set for this shell execution
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void executeSh(String command, String[] env) throws IOException,
	InterruptedException {

		logger.debug("Starting execution of command '" + command + "'");
		Process p = Runtime.getRuntime().exec(command, env);

		logger.debug("Reading output stream...");
		InputStream is = p.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String in = "";
		do {
			logger.debug(in);
			in = br.readLine();
		} while (in != null);

		logger.debug("Reading error stream");
		InputStream es = p.getErrorStream();
		InputStreamReader esr = new InputStreamReader(es);
		BufferedReader ebr = new BufferedReader(esr);
		String ein = "";
		do {
			logger.debug(ein);
			ein = ebr.readLine();
		} while (ein != null);

		int ret = p.waitFor();

		if(ret != 0) {
			throw new RuntimeException("Command '" + command + "' exited with non zero return code. Return code is = " + ret + "\nCheck the log file, " + logFile);
		} else {
			logger.info("Process successfully executed.");
		}
	}
}
