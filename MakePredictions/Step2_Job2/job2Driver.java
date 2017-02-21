package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class job2Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new job2Driver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception{

		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);

		}

		/*
		 * Instantiate a Job object for your job's configuration.  
		 */
		Job job = new Job(getConf());

		job.setJarByClass(job2Driver.class);
		job.setJobName("job2");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(job2Mapper.class);
		job.setReducerClass(job2Reducer.class);

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
}
