/* Java imports */
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
/* Spark imports */
import scala.Tuple2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

@SuppressWarnings("unused")
public class SparkCovid19_1 {

    
    @SuppressWarnings("resource")
	public static void main(String[] args) throws Exception{
	
	String pattern = "yyyy-MM-dd";
	SimpleDateFormat sdf = new SimpleDateFormat(pattern);
	Date start_date = sdf.parse(args[1].trim());
	Date end_date = sdf.parse(args[2].trim());
	Date first_date = sdf.parse("2019-12-31");
	Date last_date = sdf.parse("2020-04-08");
	int flag = 0;
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
	String output = args[3];
		
	SparkConf conf = new SparkConf().setAppName("CSE532").setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> dataRDD = sc.textFile(args[0]);

	@SuppressWarnings("serial")
	JavaPairRDD<String, Integer> counts =
		dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
			public Iterator<Tuple2<String, Integer>> call(String value) throws ParseException{
			String[] words = value.split(",");
			int flag = 0;
				
			List<Tuple2<String, Integer>> retWords =
			     new ArrayList<Tuple2<String, Integer>>();
				
			if (words[0].contains("date")) {
				flag=1;
			}
			if (flag==0){
				Date date = sdf.parse(words[0]);
				
				if(date.compareTo(start_date)<0 || date.compareTo(end_date)>0) {
					flag = 1;
				}
				else {
					retWords.add(new Tuple2<String, Integer>(words[1], Integer.parseInt(words[3])));
				}
				
			}
			return retWords.iterator();
		    }
		}).reduceByKey(new Function2<Integer, Integer, Integer>(){
			public Integer call(Integer x, Integer y){
			    return x+y;
			}
			});
		
	counts.saveAsTextFile(output);
		
	}
}