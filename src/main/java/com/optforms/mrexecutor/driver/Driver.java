package com.optforms.mrexecutor.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.optforms.mrexecutor.mr.ExecutorMapper;

/**
 * 
 * Main driver to execute certain algorithms using MapReduce framework.
 *
 */
public class Driver {

	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException, JSONException {

		Configuration conf = new Configuration();

		//Parse and set Hadoop related properties (set with -D) that are passed as arguments
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err
			.println("Usage: mrexecutor <algorithm> <properties_json_path> [data_header]");
			System.exit(2);
		}

		// Read JSON configuration file
		System.out.println("Reading properties json file...");
		FileReader confFileReader = new FileReader(new File(otherArgs[1]));
		BufferedReader br = new BufferedReader(confFileReader);

		String confStr = "";
		String line = null;

		while ((line = br.readLine()) != null) {
			confStr += line;
		}

		confFileReader.close();
		br.close();

		System.out.println("Properties json file read successfully...");

		//Parse JSON String into JSON Object and get required key-values.
		JSONObject jobConf = new JSONObject(confStr);
		JSONArray algorithms = jobConf.getJSONArray("algorithms");
		JSONObject currentAlgoJSON = null;

		for (int i = 0; i < algorithms.length(); i++) {
			//Get properties JSON object for algorithm to be executed
			if (algorithms.getJSONObject(i).getString("name")
					.equalsIgnoreCase(otherArgs[0])) {
				currentAlgoJSON = algorithms.getJSONObject(i);
			}
		}

		//If properties JSON Object is null, it's not been in set in properties file, abort.
		if (currentAlgoJSON == null) {
			System.out.println("FATAL: Configuration for algorithm '"
					+ otherArgs[0]
							+ "', could not be found in configuration file, '"
							+ otherArgs[1] + "'. Aborting.");
			System.exit(1);
		}

		// Set algorithm specific values from config JSON
		conf.set("OUT_DIR", currentAlgoJSON.getString("hdfs_out_dir"));
		conf.set("ALGO_BIN_HOME", currentAlgoJSON.getString("binary_dir"));

		String data_header = null;
		if (otherArgs.length == 3) {
			data_header = otherArgs[2];
		}

		if(data_header == null) {
			BufferedReader cmdLineReader = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("\nPlease enter header for data files in '" + currentAlgoJSON.getString("hdfs_in_dir") + "' directory on HDFS: ");
			data_header = cmdLineReader.readLine();
			cmdLineReader.close();
		}

		//Commented following to always add data header and read it from arguments or user and not from properties file
		//conf.setBoolean("ADD_DATA_HEADER", currentAlgoJSON.getBoolean("add_data_header"));
		//conf.set("DATA_HEADER", currentAlgoJSON.getString("data_header"));

		conf.setBoolean("ADD_DATA_HEADER", true);
		conf.set("DATA_HEADER", data_header);

		//Get the list of commands that are to be executed as pert of this algorithm
		JSONArray executables = currentAlgoJSON.getJSONArray("executables");
		String[] cmd = new String[executables.length()];
		for (int i = 0; i < executables.length(); i++) {
			cmd[i] = executables.getJSONObject(i).getString("command");
		}
		conf.setStrings("COMMANDS", cmd);

		// Set generic values from config JSON
		conf.set("STAGE_DIR", jobConf.getString("stage_dir"));
		conf.set("MCR_ROOT", jobConf.getString("mcr_root"));
		conf.set("MCR_CACHE_ROOT", jobConf.getString("mcr_cache_root"));

		//Set Job properties
		Job job = Job.getInstance(conf);
		job.setJobName(otherArgs[0] + "-MR-Executor");
		job.setJarByClass(Driver.class);
		job.setMapperClass(ExecutorMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//Set input file path. This path should be HDFS one.
		//Mappers will write input splits from this input file to a staging area on local file system.
		//These staged input files will then be given to algorithm, executables.
		FileInputFormat.addInputPath(job,
				new Path(currentAlgoJSON.getString("hdfs_in_dir")));

		//Algorithm executables will produce output files in staging area which will be copied to HDFS
		//directory represented by below path.
		FileOutputFormat.setOutputPath(job,
				new Path(currentAlgoJSON.getString("hdfs_out_dir")));

		System.out.println("Submitting job on the cluster...");
		int success = job.waitForCompletion(true) ? 0 : 1;

		if(success==0) {
			System.out.println("Job completed successfully...");
		} else {
			System.out.println("Job failed. Aborting.");
			System.exit(1);
		}

		//Get handler to HDFS output directory
		FileSystem fs = FileSystem.newInstance(conf);
		RemoteIterator<LocatedFileStatus> i = fs.listFiles(new Path(
				currentAlgoJSON.getString("hdfs_out_dir")), false);

		System.out.println("Cleaning up part files from hdfs output directory...");
		while (i.hasNext()) {
			LocatedFileStatus f = i.next();
			//Delete all the 'part' files generated by Mappers.
			//These part files are empty and do not contain output.
			//Actual output is contained by txt files written by algorithm executables
			if (f.isFile() && f.getPath().getName().startsWith("part-")) {
				fs.delete(f.getPath(), true);
			}
		}
		fs.close();

		System.out.println("Done. Exiting.");
		System.exit(success);
	}
}