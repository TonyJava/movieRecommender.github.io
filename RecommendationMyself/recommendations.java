import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.TreeMap;


public class recommendations {
	//Create a file to store the results.
	static File file = new File("src/recomemdations.txt");
	static String final_output = "";
	
	public static void main(String[] args) throws IOException
	{
		
		//System.out.print("Enter number of recommendations : ");
		//BufferedReader br2 = new BufferedReader(new InputStreamReader(System.in));
		//int no_of_recommendations = Integer.parseInt(br2.readLine());
		int no_of_recommendations = 10;
		//br2.close();
		
		//System.out.print("Enter user id (1,2,3): ");
		//BufferedReader br3 = new BufferedReader(new InputStreamReader(System.in));
		//int cust_id = Integer.parseInt(br2.readLine());
		int cust_id = 111222;  //Right
		//br2.close();
		
		//lets read movie names and store it in map which will be used for recommendation
		BufferedReader movie_names_buffer = new BufferedReader(new FileReader("src/movie_Ids.txt"));
		Map<Integer,String> unwatched_movieid_name = new HashMap<Integer,String>();
		String Str2;
		
		while((Str2 = movie_names_buffer.readLine()) != null)
		{
			String[] splits = Str2.split("\t");
			//System.out.println(Str2); //Read files directly.
			/*
			 * Split the movie_Ids file, and the format should be (movie_Id, movie_Name).
			 * Example: 
			 * 	28	Lilo and Stitch
			 * 	64	Outside the law
			 * 	66	Barbarian Queen 
			 * (The separator is tab.
			 */
			//System.out.println("unwatched "+splits[1].trim());
			unwatched_movieid_name.put(Integer.parseInt(splits[0].trim()),splits[1].trim());
			//System.out.println(unwatched_movieid_name.get(Integer.parseInt(splits[0].trim()))); /* The HashSet is created rightly*/
			//System.out.println(Integer.parseInt(splits[0].trim()));
		}
		movie_names_buffer.close();
		/////////////////////////////////////////////////////////////////
		
		ArrayList<String> movies_watched_by_cust = new ArrayList<String>();
		
		/*The elements of the watched_movie_rating should be <cust_Id, movie_Id>.
		 * Note, the movie_Title field is lost because we don't use it.
		 * */
		Map<Integer, Double> watched_movie_rating = new HashMap<Integer,Double>();
		
		//Read the Testing data.
		BufferedReader br = new BufferedReader(new FileReader("src/TestingRatings_Personal.txt"));
		
		final_output+="Already Watched Movies \n";
		//lets find out user's already watched movies and ratings
		String Str;
		while((Str = br.readLine()) != null)
		{
			//String Str = br.readLine();
			//System.out.println(Str);
			String[] splits = Str.split(",");
			
			if(Integer.parseInt(splits[1].trim()) == cust_id)
			{
				movies_watched_by_cust.add(splits[0]);
				//System.out.println(splits[0]);
				final_output+=" "+splits[0].toString().trim()+" =";
				// System.out.println(final_output); // This line is correct
				//Look the movie_Title by using the movie_Id. 
				final_output+=" "+unwatched_movieid_name.get(Integer.parseInt(splits[0]))+"  ";
				// System.out.println(unwatched_movieid_name.get(Integer.parseInt(splits[0]))); // This line is correctly
				
				//Put the watched movie_Id and the movie_ratings into the unwatched_movieid input.
				watched_movie_rating.put(Integer.parseInt(splits[0]),Double.parseDouble(splits[2]));
				
				// Remove the movie_Ids that the user have rated.
				unwatched_movieid_name.remove(splits[0]);  //remove watched movie from map as it is going to give Recommendations
			}
		}
		br.close();
		final_output+=" \n";
		/**This block is correct.**/
		//now we have particular users movie and rating given by him
		
		//lets say customer has watched 1000 number movie
		
		//now we need to find those pairs from reducer2 output which contain movies watched by user
		
		/*this file looks like
		1000~1	0.9756098
		1000~100	0.96827734
		1000~1001	0.9356015
		1000~1002	1.0
		1000~1003	1.0
		1000~1007	1.0
		1000~101	0.8944272*/
		
		TreeMap<String,Float> recommendations = new TreeMap<String,Float>();
		
		String movie_name = "random";
		for(Entry<Integer, String> entry : unwatched_movieid_name.entrySet())
		{	
			BufferedReader sim_score_buff = new BufferedReader(new FileReader("src/similarities_personal.txt"));
			
			
			float numerator = 0; 
			float denominator = 0;
			Integer Unwatched_movieId = entry.getKey();
			String LineRead;
			//Double r = 0.0;
			//float similarity = 0;
			
			while((LineRead =sim_score_buff.readLine())!=null)
			{
				String[] movies = LineRead.split("\\s")[0].split(",");
				//System.out.println(LineRead.split("\\s")[0]);  // Correct
				//System.out.println(LineRead); //This line is right.
				//Input: [10006,13905 0]
				if(LineRead.contains(Unwatched_movieId.toString().trim()) && (movies_watched_by_cust.contains(movies[0].trim()) || movies_watched_by_cust.contains(movies[1].trim())))
				{
					movie_name = entry.getValue(); // Right
					//System.out.println(movie_name);
					String rating = LineRead.split("\\s")[1];
					//System.out.println(rating);  //Correct
					float similarity = Math.abs(Float.parseFloat(rating)); // Get the absolute value
					//System.out.println(similarity); //Correct
					Double r = 0.0;
					if(movies_watched_by_cust.contains(movies[0])){
						//System.out.println(movies[0]);//Correct
						//r = Math.abs(watched_movie_rating.get(Integer.parseInt(movies[0].trim())));
						//System.out.println(r);
						r = watched_movie_rating.get(Integer.parseInt(movies[0].trim()));
					}
					else{
						r = watched_movie_rating.get(Integer.parseInt(movies[1].trim()));
						}
					numerator += (similarity*r); //(similarity*rating) present in numerator
					//System.out.println(numerator); // This line is wrong.
					
					if(similarity<0){denominator += (-1*similarity);}  //take care of |similarity| in denominator
					else{denominator += similarity;}
				}
			}/**This block is correct**/
			
			
			float weighted_sum = numerator/denominator;
			if((weighted_sum !=0.0 && !Float.isNaN(weighted_sum))&& !movie_name.equals("random"))
			{
				//final_output+="\nCould Recommend - "+movie_name+" with "+weighted_sum;
				final_output+="\n" +movie_name+","+weighted_sum;
				recommendations.put(movie_name,weighted_sum);
			}
			sim_score_buff.close();
		}//end of FOR loop
		
		//Recommend the top "no_of_recommendations" of the movies.
		sort(recommendations,no_of_recommendations);
		write_to_file(final_output);
		System.out.println("All done");
	}//end of main function
	
	//lets sort the result in descending to get top k recommendations
	public static void sort(TreeMap<String, Float> recommendations, int no_of_recommendations) throws IOException
	{
		TreeSet<String> ts = new TreeSet<String>();
		for(Entry<String, Float> entry : recommendations.entrySet())
		{
			ts.add(entry.getValue()+"_"+entry.getKey());
		}
		
		Iterator<String> iterator;
	    iterator = ts.descendingIterator();
	    
	    int i=0;
	    String output="";
	    while(i!=no_of_recommendations && iterator.hasNext())
	    {
	    	String[] splits = iterator.next().split("_");
	    	output+=splits[1];
	    	i=i+1;
	    	if(i<no_of_recommendations){output+=",";}
	    }
	    final_output+=" \n \n------------------------- \n";
	    final_output+="Final Recommended top movies are \n";
	    final_output+=output;
	}
	
	//below part is to write final output to 'final_recommendation' file



	public static void write_to_file(String input) throws IOException
	{	
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.append(input);
		bw.close();
	}
	
}//end of class
