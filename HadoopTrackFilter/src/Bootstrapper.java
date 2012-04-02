import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;



public class Bootstrapper 
{
	public static void main(String[] args) throws Exception
	{
		JobConf c = new JobConf(Bootstrapper.class);
		c.setJobName("7dizzle_mapredu");
		
		c.setMapperClass(SevenDizzleMapper.class);
		c.setReducerClass(SevenDizzleReducer.class);
		
		c.setMapOutputKeyClass(Text.class);
		c.setMapOutputValueClass(Text.class);
		
		c.setInputFormat(TextInputFormat.class);
		c.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(c, new Path(args[0]));
		FileOutputFormat.setOutputPath(c, new Path(args[1]));
		
		JobClient.runJob(c);
	}
}
