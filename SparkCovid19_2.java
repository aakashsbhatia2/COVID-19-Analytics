/* Java imports */
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
import java.util.*;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
@SuppressWarnings("unused")
public class SparkCovid19_2 {

    
    @SuppressWarnings("serial")
	public static void main(String[] args){

	String input = args[0];
	String output = args[2];
	
	SparkConf conf = new SparkConf().setAppName("CSE532").setMaster("local");;
	@SuppressWarnings("resource")
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> dataRDD = sc.textFile(args[0]);
	JavaRDD<String> dataRDD_2 = sc.textFile(args[1]);
	
	
	JavaPairRDD<String, Double> countries_pairs =
		    dataRDD_2.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
			    public Iterator<Tuple2<String, Double>> call(String value){
			    	String[] countries_entries = value.split(",");
				
			    	List<Tuple2<String, Double>> map_vals = new ArrayList<Tuple2<String, Double>>();
			    	try {
			    		map_vals.add(new Tuple2<String, Double>(countries_entries[1], Double.parseDouble(countries_entries[4])));
			    	}
			    	catch(Exception e){
			    		
			    	}
			    	return map_vals.iterator();
			    }
		    });
			
			Map<String,Double> map =  countries_pairs.collectAsMap();
			HashMap<String, Double> countries_map = new HashMap<String, Double>(map);
	
			Broadcast<HashMap<String, Double>> bc = sc.broadcast(countries_map);
			
			
			    
			    
	JavaPairRDD<String, Double> counts =
	    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
		    public Iterator<Tuple2<String, Double>> call(String value){
		    	String[] words = value.split(",");
		    	int flag = 0;
		    	List<Tuple2<String, Double>> retWords = new ArrayList<Tuple2<String, Double>>();
		    	
		    	if(words[0].contains("date") || countries_map.get(words[1].toString())==null) {
		    		flag = 1;
		    	}
		    	if (flag ==0) {
		    		retWords.add(new Tuple2<String, Double>(words[1], Double.parseDouble(words[2])));
		    	}
		    	return retWords.iterator();
		    }
		}).reduceByKey(new Function2<Double, Double, Double>(){
			public Double call(Double x, Double y){
				return x+y;
			}
		});
	
	JavaPairRDD<String, Double> out =
		    counts.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Double>, String, Double>(){

				@Override
				public Iterator<Tuple2<String, Double>> call(Tuple2<String, Double> value) throws Exception {
					String country = value._1;
					Double cases = value._2;
					Double new_cases = 0.0;
			    	List<Tuple2<String, Double>> result = new ArrayList<Tuple2<String, Double>>();
					new_cases = cases/countries_map.get(country)*1000000;
					result.add(new Tuple2<String, Double>(country, new_cases));
			    	return result.iterator();
				}
		    	
		    });
	out.saveAsTextFile(output);

    }
}