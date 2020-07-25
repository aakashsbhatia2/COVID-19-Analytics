import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

@SuppressWarnings("unused")
public class Covid19_1 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String world = conf.get("world");
			int flag = 0;
			String[] result = value.toString().split(",");
			
			if (result[0].contains("2019") || result[0].contains("date")) {
				flag = 1;
			}
			if(world == "false") {
				if (result[1].contains("World") || result[1].contains("International")) {
				flag = 1;
				}
			}
			if(flag == 0) {
				word.set(result[1]);
				LongWritable count = new LongWritable(Long.parseLong(result[2]));
				context.write(word, count);
			}
			
		}
		
	}
	
	

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		private LongWritable total = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			long sum = 0;
			
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		
		Configuration conf = new Configuration();
		
		if (args[1].contains("true")) {
			conf.set("world", "true");
		}
		else {
			conf.set("world", "false");
		}
		
		Job myjob = Job.getInstance(conf, "Covid19_1");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}