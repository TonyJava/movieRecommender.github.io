package jobPrediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* receive (UserId, list of (MovieId,Rating))
 * emit (PredictingMovieID, PredictingUserID, actualRating, predictedRating)
 * */

public class job3Reducer extends Reducer<Text, Text, Text, Text> {
	private Text Composite_Key = new Text();
	private Text Composite_Value = new Text();
	private String DataFileName = "/home/training/Desktop/TestingRatings.txt";
	/* Loading the input File*/
	private String simFileName = "/home/training/Desktop/similarities.txt";
	/*Loading the similarities*/ 
	/* userId -> (movieId,rating) */
	private Map<Integer, List<Entry>> testingEntries = new HashMap<Integer, List<Entry>>();
	/*
	 * movieId1 -> movieId2 -> rating
	 */
	private Map<Integer, Map<Integer, Float>> movieSimilarities = new HashMap<Integer, Map<Integer, Float>>();
	// Define the Entry, and it is the movieId, rating. It stores the list of <movie, rating>.
	class Entry {
		int movie_Id;
		float rating;

		public Entry(int movie_Id, float rating) {
			this.movie_Id = movie_Id;
			this.rating = rating;
		}
	}

	class SimRatingPair {
		float similarity;
		float rating;

		SimRatingPair(float similarity, float rating) {
			this.similarity = similarity;
			this.rating = rating;
		}
	}

	/* Override the setup and initialize the TestingData and the SimData. */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		initTestingData();
		initSimData();
	}
	/*
	 * initialize the testing data. Load all data from TestingRatings.txt into
	 * testingEntries
	 */
	private void initTestingData() throws FileNotFoundException, IOException {
		/*
		 * movieId, userId, rating
		 */
		File testingDataFile = new File(DataFileName);
		BufferedReader br = new BufferedReader(new FileReader(testingDataFile));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(",");
			int userId = Integer.parseInt(tokens[1]), movieId = Integer
					.parseInt(tokens[0]);
			float rating = Float.parseFloat(tokens[2]);

			if (testingEntries.containsKey(userId)) {
				testingEntries.get(userId).add(new Entry(movieId, rating));
			} else {
				List<Entry> temp = new ArrayList<Entry>();
				temp.add(new Entry(movieId, rating));
				testingEntries.put(userId, temp);
			}
		}
		br.close();
	}

	/*
	 * Load data from similarities.txt into moviesSimilarities
	 */
	private void initSimData() throws FileNotFoundException, IOException {
		/*
		 * (item1,item2 \t rating)
		 */
		File simFile = new File(simFileName);
		try {
			BufferedReader br = new BufferedReader(new FileReader(simFile));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int item1 = Integer.parseInt(tokens[0].split(",")[0]), item2 = Integer
						.parseInt(tokens[0].split(",")[1]);
				float sim = Float.parseFloat(tokens[1]);

				/*
				 * item1 is always smaller than item2 in the nested map !! i.e.
				 * the key of outer map must be smaller than the key of the
				 * inner map. To get ratings, use movieSimilarities.get(smaller
				 * item-id).get(larger item-id)
				 */
				if (item1 > item2) {
					int temp = item1;
					item1 = item2;
					item2 = temp;
				}

				if (movieSimilarities.containsKey(item1)) {
					movieSimilarities.get(item1).put(item2, sim);
				} else {
					Map<Integer, Float> temp = new HashMap<Integer, Float>();
					temp.put(item2, sim);
					movieSimilarities.put(item1, temp);
				}

			}
			br.close();
		} catch (Exception e) {
			System.out.println("Could not open file");
			return;
		}
	}

	/* Order the pair to reduce redundancy. */
	static final Comparator<SimRatingPair> SIM_PAIR_ORDER = new Comparator<SimRatingPair>() {
		public int compare(SimRatingPair s1, SimRatingPair s2) {
			return Float.compare(s1.similarity, s2.similarity);
		}
	};

	/* In the following part, we want predict the rating by using weighted average */
	private float weightedAvg(int movie_Id, List<Entry> ratings) {
		
		int N = 40;
		float sum = 0, num_of_common = 0;

		PriorityQueue<SimRatingPair> queue = new PriorityQueue<SimRatingPair>(
				N, SIM_PAIR_ORDER);

		for (Entry e : ratings) {

			float similarity;

			if (movieSimilarities.containsKey(Math.min(movie_Id, e.movie_Id))
					&& movieSimilarities.get(Math.min(movie_Id, e.movie_Id))
							.containsKey(Math.max(movie_Id, e.movie_Id))) {
				similarity = movieSimilarities.get(Math.min(movie_Id, e.movie_Id)).get(
						Math.max(movie_Id, e.movie_Id));
			} else {
				similarity = 0; 
				/*If movie1 and movie 2 don't elements in common, 
				 * we set the similarity between them as 0 and item */
			}

			if (queue.size() < N) {
				queue.add(new SimRatingPair(similarity, e.rating));
			} else if (Float.compare(queue.peek().similarity, similarity) < 0) {
				queue.remove();
				queue.add(new SimRatingPair(similarity, e.rating));
			}

		}

		for (SimRatingPair i : queue) {
			sum += Math.abs(i.similarity) * i.rating;
			num_of_common += Math.abs(i.similarity);
			//num_of_common += 1;
			//num_of_common += i.similarity;
			System.out.println(num_of_common);
		}

		return Math.abs(sum / num_of_common);
	}


	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/* retrieve all ratings rated by the testing user */
		List<Entry> ratings = new ArrayList<Entry>();
		for (Text value : values) {
			String[] tokens = value.toString().split(",");
			ratings.add(new Entry(Integer.parseInt(tokens[0]), Float
					.parseFloat(tokens[1])));
		}
		/*
		 * We use the similarity as the rule to weight the ratings of the user
		 * and then calculate the predicted ratings by weighting the ratings of
		 * the user.
		 */
		for (Entry e : testingEntries.get(Integer.parseInt(key.toString()))) {
			float p = weightedAvg(e.movie_Id, ratings);
			

			/*
			 * K2.set(String.valueOf(e.rating)); V2.set(String.valueOf(p));
			 */

			Composite_Key.set(e.movie_Id + "," + key.toString());
			//Composite_Value.set(String.format("TrueValue: %.4f, PredictedValue %.4f", e.rating, p));
			Composite_Value.set(String.format("%.4f, %.4f", e.rating, p));

			context.write(Composite_Key, Composite_Value);
		}
	}
}
