import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.text.ParseException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

@SuppressWarnings("unused")
public class Covid19_2 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String start_date = conf.get("start_date");
			String end_date = conf.get("end_date");
			String pattern = "yyyy-MM-dd";
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			int flag = 0;
			String[] result = value.toString().split(",");
			String date_temp = result[0]; 
			Date start_date_parsed = null; 
			Date end_date_parsed= null; 
			Date date= null;
			
			if (result[0].contains("date")) {
				flag = 1;
			}
			if(flag == 0) {
				try {
					start_date_parsed = sdf.parse(start_date);
					System.out.println(start_date_parsed);
					end_date_parsed = sdf.parse(end_date);
					System.out.println(end_date_parsed);
					date = sdf.parse(date_temp);
					System.out.println(date);
				} 
				catch (ParseException e) {
					System.out.println("Exception raised");
				}
				if (date.compareTo(start_date_parsed)<0 || date.compareTo(end_date_parsed)>0) {
					flag = 1;
				}
			}
			if(flag == 0) {
				word.set(result[1]);
				LongWritable count = new LongWritable(Long.parseLong(result[3]));
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
		String pattern = "yyyy-MM-dd";
		
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Date start_date = sdf.parse(args[1].trim());
		Date end_date = sdf.parse(args[2].trim());
		Date first_date = sdf.parse("2019-12-31");
		Date last_date = sdf.parse("2020-04-08");
				
		if(start_date.compareTo(end_date)>0) {
			System.out.println("Start Date after end date");
			System.exit(1);
		}
		if(start_date.compareTo(first_date)<0) {
			System.out.println("Date out of range");
			System.exit(1);
		}
		if(end_date.compareTo(last_date)>0) {
			System.out.println("Date out of range");
			System.exit(1);
		}
		
		conf.set("start_date", args[1].trim());
		conf.set("end_date", args[2].trim());
		Job myjob = Job.getInstance(conf, "my word count test");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}