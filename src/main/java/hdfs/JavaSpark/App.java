package hdfs.JavaSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class App {

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("sampleAnalyzer").setMaster("local[2]");

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			System.out.println("Welcome to Spark Program");
//			JavaRDD<String> lines = sc.textFile("sampleRestaurantdata.csv");
			JavaRDD<String> lines = sc.textFile("samplecsv.csv");
			JavaRDD<String> city = lines.filter(new Function<String, Boolean>() {

				@Override
				public Boolean call(String v1) throws Exception {
					String[] arr = v1.split(",");
					String city = arr[3];
					if (city.equalsIgnoreCase("TX"))
						return true;
					return false;
				}
			});

			JavaPairRDD<Tuple2<String, Integer>, Iterable<String>> cityGroup = lines
					.groupBy(new Function<String, Tuple2<String, Integer>>() {

						@Override
						public Tuple2<String, Integer> call(String v1) throws Exception {
							String[] arr = v1.split(",");
							return new Tuple2(arr[3], 1);
						}

					});

			cityGroup.saveAsTextFile("output");
		}

	}
}
