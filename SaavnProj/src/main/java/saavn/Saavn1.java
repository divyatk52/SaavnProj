package saavn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Saavn1 {
	public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> 
	{

 		public void map(Object key, Text record, Context con) throws IOException, InterruptedException 
		{
  			String[] info = record.toString().split(",");
			String sid = info[0];
  			String date = info[4];
  			if(date.contentEquals("2017-12-01"))
   			 con.write(new Text(sid), new IntWritable(1));
 		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, IntWritable> 
	{
		//int count = 0;

 		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
 			

 			int count = 0;

 			for (@SuppressWarnings("unused")
 			IntWritable value : values) {
 				count++;
 			}

 			context.write(key, new IntWritable(count));
 		}
	}
	public static int main(String[] args) throws Exception 
	{
		
		Configuration conf;
		Job job;
			
    		conf = new Configuration();
			
    		job = Job.getInstance(conf, "Saavn1");
    		job.setJarByClass(Saavn1.class);
			
    		job.setMapperClass(MapperClass.class);
    		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));

    		try {
    			return job.waitForCompletion(true) ? 0 : 1;
    		} catch (ClassNotFoundException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		} catch (InterruptedException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		return 0;
  	}

}
