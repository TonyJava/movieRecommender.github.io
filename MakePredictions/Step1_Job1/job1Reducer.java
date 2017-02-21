package job1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class job1Reducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  SortedMap <Integer, String> map = new TreeMap<Integer,String>();
	  	int count = 0;
		for (Text value : values) {
		  	String[] input = value.toString().split("\\s+");
		  	StringBuilder build = new StringBuilder();
		  	//The first variable is usr_id, and we retrieve it and put it to the map.
		  	//The second variable should be a composite value, hence the build(v1, v2).
		  	int user_Id = Integer.parseInt(input[0]);
		  	for(int i =0 ; i< input.length;i++){
		  		if(i!=0){
		  			build.append(input[i]);
		  			build.append("  ");
		  		}
		  	}
		  	String out = build.toString();
		  	map.put(user_Id,out);
		  	count++;
		}
		
		
		List<Integer> list = new ArrayList<Integer>(map.keySet());
		for(int i =0 ; i< count; i++ ){
			for (int j =i+1; j< count; j++){
				String outputVal = map.get(list.get(i))+"  "+map.get(list.get(j));
				String output = list.get(i)+ "  " + list.get(j);
				context.write(new Text(output),new Text(outputVal) );
			}
		}
  }
}
