package hdfs.JavaSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class AppSql {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("Java Spark Sql Example").master("local[1]")
				.getOrCreate();

		StructType struct = new StructType().add("business_id", "string").add("name", "string").add("address", "string")
				.add("state", "string").add("city", "string").add("postal_code", "string").add("lattitude", "string")
				.add("longitude", "string").add("stars", "long").add("review_count", "long").add("is_open", "long")
				.add("categories", "string").add("hours", "string").add("review_id", "string").add("user_id", "string")
				.add("customers_stars", "long").add("userful", "long").add("funny", "long").add("cool", "long")
				.add("text", "string").add("date", "string");

		Dataset<Row> df = sparkSession.read().schema(struct).option("header", true).option("delimiter", "|")
				.csv("samplecsv.csv");

		df.createOrReplaceTempView("reviews");
		//
		Dataset<Row> result = sparkSession
				.sql("SELECT state, sum(review_count) as count FROM reviews GROUP BY state ORDER BY count DESC");

		result.show();
		result.write().option("header", true).csv("./SparkOutput/topStateWiseRestaurant");

		
		Dataset<Row> topCities = sparkSession.sql(
				"SELECT city,SUM(review_count) AS total_reviews FROM reviews GROUP BY city ORDER BY total_reviews DESC limit 10 ");

		topCities.show();

		topCities.write().option("header", true).csv("./SparkOutput/top10Cities");

		Dataset<Row> topRestaurants = sparkSession.sql(
				"SELECT name,SUM(review_count) as total_count, AVG(stars) as avg_stars FROM (SELECT * FROM reviews where stars IS NOT NULL) GROUP BY name ORDER BY total_count desc");

		topRestaurants.show();

		topRestaurants.write().option("header", true).csv("./SparkOutput/topRestaurantPostCovid");
		Dataset<Row> ratingCount = sparkSession.sql(
				"SELECT customers_stars, count(customers_stars) FROM reviews GROUP BY customers_stars ORDER BY customers_stars DESC");

		ratingCount.show();
		ratingCount.write().option("header", true).csv("./SparkOutput/ratingCount");

	}

}
