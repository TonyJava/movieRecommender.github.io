package job2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class job2Reducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

	  int count = 0;
	  double sum = 0;
	  double demoX = 0, demoY=0;
		for (Text value : values) {
		  	String[] input = value.toString().split("\\s+");
		  	double user1_rating = Double.parseDouble(input[0]);
		  	double user1_avg = Double.parseDouble(input[2])/Double.parseDouble(input[1]);
		  	double user1_sum = Double.parseDouble(input[2]);
		  	double user2_rating = Double.parseDouble(input[3]);
		  	double user2_avg = Double.parseDouble(input[5])/Double.parseDouble(input[4]);
		  	double user2_sum = Double.parseDouble(input[5]);
		  	double rxs = user1_rating - user1_avg;
		  	double rys = user2_rating - user2_avg;
		  	double nomi = rxs * rys;
		  	double sqr1 = rxs*rxs;
		  	double sqr2 = rys*rys;
		  	sum += nomi;
		  	demoX+= sqr1;
		  	demoY+= sqr2;
		  	count++;
		}
		if(count < 2||(Math.sqrt(demoY)*Math.sqrt(demoX) ==0)) context.write(key, new Text("0"));
		else {
			double PearsonCorr = sum/(Math.sqrt(demoY)*Math.sqrt(demoX));
			double PC_Final = Math.abs(PearsonCorr);
			context.write(new Text(key),new Text(PC_Final+""));
		}
  }
}
