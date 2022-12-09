package hdfs.JavaSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class AppSql {

	public static void main(String[] args) {

		AppSql sql = new AppSql();
		sql.covidAnalysis("postcovid_reviews.csv", "postCovid");
		sql.covidAnalysis("precovid_reviews.csv", "precovid");

	}

	private void covidAnalysis(String fileName, String outputPath) {

		SparkSession sparkSession = SparkSession.builder().appName("Java Spark Sql Example").master("local[2]")
				.getOrCreate();

		StructType struct = new StructType().add("business_id", "string").add("name", "string").add("address", "string")
				.add("state", "string").add("city", "string").add("postal_code", "string").add("lattitude", "string")
				.add("longitude", "string").add("stars", "long").add("review_count", "long").add("is_open", "long")
				.add("categories", "string").add("hours", "string").add("review_id", "string").add("user_id", "string")
				.add("customers_stars", "long").add("useful", "long").add("funny", "long").add("cool", "long")
				.add("date", "string");


		Dataset<Row> df = sparkSession.read().schema(struct).option("header", true).option("delimiter", ",")
				.csv("s3://mysparkbucket02/Databases/" + fileName);
		
		df.createOrReplaceTempView("reviews");
	
		Dataset<Row> result = sparkSession
				.sql("SELECT state, COUNT(review_count) as count FROM reviews GROUP BY state ORDER BY count DESC");

		result.show();
		result.write().option("header", true).csv(outputPath + "/topStateWiseRestaurant");

		Dataset<Row> topCities = sparkSession.sql(
				"SELECT city,COUNT(review_count) AS total_reviews FROM reviews GROUP BY city ORDER BY total_reviews DESC limit 10 ");

		topCities.show();

		topCities.write().option("header", true).csv(outputPath + "/top10Cities");

		Dataset<Row> topRestaurants = sparkSession.sql(
				"SELECT name,COUNT(review_count) as total_count, AVG(stars) as avg_stars FROM (SELECT * FROM reviews where stars IS NOT NULL) GROUP BY name ORDER BY total_count desc");

		topRestaurants.show();

		topRestaurants.write().option("header", true).csv(outputPath + "/topRestaurantPostCovid");
		Dataset<Row> ratingCount = sparkSession.sql(

		
		"SELECT customers_stars, count(customers_stars) FROM reviews GROUP BY customers_stars ORDER BY customers_stars DESC");

		ratingCount.show();
		ratingCount.write().option("header", true).csv(outputPath + "/ratingCount");

		Dataset<Row> topUseful = sparkSession.sql(
				"SELECT name,SUM(useful) AS total_useful FROM reviews GROUP BY name ORDER BY total_useful DESC limit 10 ");

		topUseful.show();

		topUseful.write().option("header", true).csv(outputPath + "/top10UsefulRestaurents");

		Dataset<Row> openRestaurents = sparkSession.sql(
				"SELECT city,COUNT(is_open) AS total_open FROM reviews where is_open=1 GROUP BY city ORDER BY total_open DESC limit 10 ");

		openRestaurents.show();

		openRestaurents.write().option("header", true).csv(outputPath + "/top10CityOpenRestaurents");
		
		Dataset<Row> closeRestaurents = sparkSession.sql(
				"SELECT city,count(*) AS stars FROM reviews WHERE is_open = '0' GROUP BY city ORDER BY stars DESC limit 10 ");
		closeRestaurents.show();

		closeRestaurents.write().option("header", true).csv(outputPath + "/top10CityCloseRestaurents");
		
		Dataset<Row> topFunny = sparkSession.sql(
				"SELECT name,SUM(funny) AS total_funny FROM reviews GROUP BY name ORDER BY total_funny DESC limit 10 ");
		topFunny.show();

		topFunny.write().option("header", true).csv(outputPath + "/top10FunnyRestaurents");
		
		Dataset<Row> topCool = sparkSession.sql(
				"SELECT name,SUM(cool) AS total_cool FROM reviews GROUP BY name ORDER BY total_cool DESC limit 10 ");

		topCool.show();

		topCool.write().option("header", true).csv(outputPath + "/top10CoolRestaurents");
		
		Dataset<Row> indianRestaurents = sparkSession.sql(
				"SELECT city,count(*) AS stars FROM reviews WHERE categories LIKE '%Indian%' GROUP BY city ORDER BY stars DESC limit 10 ");
		indianRestaurents.show();

		indianRestaurents.write().option("header", true).csv(outputPath + "/top10CityIndianRestaurents");
		
	}

}
