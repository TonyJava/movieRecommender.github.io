package job1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  String[] input = line.split("\\s+");
	  
	  String Key = input[1];
	  StringBuilder builder = new StringBuilder();
	  for(int i =0 ; i< input.length ; i++){
		  if(i!=1){
			  builder.append(input[i]);
			  builder.append("   ");
		  }
	  }
	  //	output (user_id, (movie_id, rating))
        context.write(new Text(Key), new Text(builder.toString()));
  }
}
