import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class SevenDizzleReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{

	private int reduceCount = 0;

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{
		reduceCount++; // Keep track of how parallelized the reduce operation is
		
		try
		{
			
			while (values.hasNext())
			{
				String row = values.next().toString();
				if (getArtist(row).contains("The"))
				{
					output.collect(getKey(row), new Text(row + " reduce: " + reduceCount)); // log for parallelization check
				}
			}
					
		}
		catch(Exception e){}
	}

	private Text getKey(String row) 
	{
		return new Text(row.split("¬")[0]);
	}

	private String getArtist(String row) 
	{
		return row.split("¬")[1];
	}

}
