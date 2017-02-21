package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 *	In this job, we split the input into the <key, value> pairs, and store
 *the pairs into a TreeMap. One thing to mention is that the value is a 
 *composite value <value1, value2>. Hence, we need to use a StringBuilder
 *to store the composite value.
 * 
 */
public class job1Driver extends Configured implements Tool{

  public static void main(String[] args) throws Exception {

	int res = ToolRunner.run(new Configuration(), new job1Driver(), args);
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
    
    job.setJarByClass(job1Driver.class);
    job.setJobName("job1");

 
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(job1Mapper.class);
    job.setReducerClass(job1Reducer.class);
   
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
