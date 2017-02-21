package jobPrediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Input: (_, (MovieId, UserId, Rating))
 * Output: (UserId, (MovieId,Rating) */

public class job3Mapper extends Mapper<Object, Text, Text, Text> {
	private Text Composite_Key = new Text();
	private Text Composite_Value = new Text();
	private Set<Integer> User_Ids = new HashSet<Integer>();
	/* Use this HashSet to store the UserId from the TestingRatings.txt*/
	private String DataFileName = "/home/training/Desktop/TestingRatings.txt";
	
	/*
	 * In this part, we split the lines by comma, and then get the movie_Id and store it 
	 * into the hashset. In the Reducer, we will lookup the HashSet to get the movie_Id.
	 */

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		File movie_Ids = new File(DataFileName);
		BufferedReader bufferReader = new BufferedReader(new FileReader(movie_Ids));
		String tokens = null;
		while ((tokens = bufferReader.readLine()) != null) {
			User_Ids.add(Integer.parseInt(tokens.split(",")[1]));
			/*Add the movie_Ids to the HashSet*/
		}
		bufferReader.close();
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String valueAsString = value.toString().trim();
		String[] tokens = valueAsString.split(",");
		if (tokens.length != 3) {
			return;
		}

		String movie_Id = tokens[0];
		String user_Id = tokens[1];
		String rating = tokens[2];
		
		/* filter out users we don't test */
		if (User_Ids.contains(Integer.parseInt(user_Id))) {
			Composite_Key.set(user_Id);
			Composite_Value.set(movie_Id + "," + rating);
			context.write(Composite_Key, Composite_Value);
		}
	}
}