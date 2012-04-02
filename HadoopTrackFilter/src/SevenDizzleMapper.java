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
			mapCount++;
			
			try
			{
				Scanner s = new Scanner(value.toString());
				while(s.hasNextLine())
				{
					String line = s.nextLine();
					output.collect(getKey(line), new Text(line.substring(0, 100) + " Map: " + mapCount));
				}
			}
			catch (Exception ex){}
		}

			private Text getKey(String line) 
			{
				//return new Text(line.split("¬")[0]);
				return new Text("1");
				
			}
		
}
