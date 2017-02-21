import java.io.*;
public class Error {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// The name of the file to open.
		String fileName = "src/N40.txt";
		int n = 0;
		double rmseSum =0;
		double maeSum =0;
		//This will reference one line at a time

		
		try{
			//FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);
			
			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = null;

			while((line = bufferedReader.readLine())!= null){
				String[] tokens = line.split("\t");
				String value = tokens[1];
				String[] input = value.split(",");
				if(!Double.isNaN(Double.parseDouble(input[1]))){
					double actual = Double.parseDouble(input[0]);
					//System.out.println(actual); This step is right
					
					
					double predicted = Double.parseDouble(input[1]);
					//double predicted = Math.abs(Double.parseDouble(input[1]));
					//System.out.println(predicted); //This output is right;
					
					/* Since the double number is quite long, and it will overflow in most
					 * case, we should round the number and only keep the two point after
					 * the point.*/
					//System.out.println((predicted - actual)); 
					//It will overflow, because the java kernel
					double difference = ((int)(predicted - actual)*10000)/10000;
					rmseSum += difference*difference;
					maeSum += ((int)(Math.abs(difference)*10000))/10000;
					//rmseSum += (predicted - actual)*(predicted - actual);
					//rmseSum += Math.pow(predicted - actual, 2);
					//System.out.println(rmseSum); // rmseSum is infinity, actually, it is wrong.
					//maeSum += Math.abs(predicted - actual);
					//System.out.println(maeSum); // maeSum is infinity, actually, it is wrong.
					n++;
					//System.out.println(n);// n is rightly calculated.
					//System.out.println(line);

					}
			}

			// Always close files.
			bufferedReader.close();
		}
		catch(FileNotFoundException ex){
			System.out.println(
					"Unable to open file'" + fileName + "'");
		}
		catch(IOException ex){
			System.out.println("Error reading file'" + fileName + "'");
		}
		
		//Get the MAE and RMSE
		//System.out.print(rmseSum);
		//System.out.print(maeSum);
		//System.out.println(n);
		double[] ret = { Math.sqrt(rmseSum / n), maeSum / n };
		System.out.printf("RMSE is: %.4f", ret[0]);
		System.out.println();
		//System.out.println("RMSE is: " +ret[0]);
		System.out.printf("MAE is: %.4f", ret[1]);
	}
}
