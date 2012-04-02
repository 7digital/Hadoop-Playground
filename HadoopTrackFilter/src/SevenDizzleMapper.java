import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SevenDizzleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
		private int mapCount = 0;

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter arg3)throws IOException 
		{
			mapCount++; // Make a not of how many times this method is called to observe parallelization
			
			try
			{
				Scanner s = new Scanner(value.toString()); // Value will be some rows from the CSV file
				while(s.hasNextLine())
				{
					String line = s.nextLine();
					output.collect(getKey(line), new Text(line.substring(0, 100) + " Map: " + mapCount)); // Note map count ^^^
				}
			}
			catch (Exception ex){}
		}

			private Text getKey(String line) 
			{
				return new Text(line.split("¬")[0]); // Track Id
			}
		
}
