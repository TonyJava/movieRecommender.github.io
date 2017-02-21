package job2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
  @Override
  public void map(LongWritable key, Text value, Context context)
  	  //org.w3c.dom.Text
      throws IOException, InterruptedException {

	  String[] input = value.toString().split("\\s+");
	  String Key = input[0]+" "+input[1];
	  String Key1 = input[1]+" "+input[0];

	  String Value = input[2]+" "+input[3] + " " + input[4]+" "+input[5]+" "+" "+input[6]+" "+input[7];
	  context.write(new Text(Key), new Text(Value));
	  context.write(new Text(Key1), new Text(Value));
  }
}
