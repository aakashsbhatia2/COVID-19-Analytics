import java.io.*;
import java.util.*;
import java.net.URI;
import java.text.DecimalFormat;
import java.math.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.filecache.*;


@SuppressWarnings({ "unused" })
public class Covid19_3 {

	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			int flag = 0;
			String[] result = value.toString().split(",");
			
			if (result[0].contains("date")) {
				flag = 1;
			}
			if(flag == 0) {
				word.set(result[1]);
				LongWritable count = new LongWritable(Long.parseLong(result[2]));
				context.write(word, count);
			}			
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, FloatWritable> {
		
		private FloatWritable total = new FloatWritable();
		HashMap<String, Double> countries = new HashMap<String, Double>();
		
		public void setup(Context context) throws IOException, InterruptedException { 

			URI[] cacheFiles = context.getCacheFiles(); 

			try { 
				String line = ""; 
				FileSystem fs = FileSystem.get(context.getConfiguration()); 
				Path getFilePath = new Path(cacheFiles[0].toString()); 
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath))); 

				while ((line = reader.readLine()) != null)  { 
					String[] data = line.split(","); 
					try {
						countries.put(data[1], Double.parseDouble(data[4]));
					}
					catch(Exception e) {
						countries.put(data[1], 0.0);
					}
				} 
			} 
			catch (Exception e) { 
				System.out.println("Unable to read the File"); 
				System.exit(1); 	
			} 
		}
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			double inf = Double.POSITIVE_INFINITY;
			long sum = 0;
			float sum_per_mil = 0;
			
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			
			try {
				sum_per_mil = (float) (sum/countries.get(key.toString()))*1000000;
				if (sum_per_mil!=inf) {
					total.set((float) (sum_per_mil));
					context.write(key,total);
				}
			}
			catch (Exception e) {
			}
		}
	}
	
	public static void main(String[] args)  throws Exception {
		
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "Covid19_3");
		
		try {
			myjob.addCacheFile(new Path(args[1]).toUri());
		}
		catch (Exception e){
			System.out.println("File not added");
			System.exit(1);
		}
		
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}