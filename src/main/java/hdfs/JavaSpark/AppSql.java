package hdfs.JavaSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class AppSql {

	//main method 
	//entry point
	public static void main(String[] args) {

		AppSql sql = new AppSql();
		//processing postcovid analysis
		//1 args is datafile & 2 args is output file location
		sql.covidAnalysis("postcovid_reviews.csv", "postCovid");
		//processing precovid analysis
		//1 args is datafile & 2 args is output file location
		sql.covidAnalysis("precovid_reviews.csv", "precovid");

	}

	//Analysis method
	private void covidAnalysis(String fileName, String outputPath) {

		//initiating spark session to process the data
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark Sql Example").master("local[2]")
				.getOrCreate();
		//creating the schema as per the csv file
		StructType struct = new StructType().add("business_id", "string").add("name", "string").add("address", "string")
				.add("state", "string").add("city", "string").add("postal_code", "string").add("lattitude", "string")
				.add("longitude", "string").add("stars", "long").add("review_count", "long").add("is_open", "long")
				.add("categories", "string").add("hours", "string").add("review_id", "string").add("user_id", "string")
				.add("customers_stars", "long").add("useful", "long").add("funny", "long").add("cool", "long")
				.add("date", "string");

		//creating the dataframe
		//location of dataset s3 URL
		Dataset<Row> df = sparkSession.read().schema(struct).option("header", true).option("delimiter", ",")
				.csv("s3://mysparkbucket02/Databases/" + fileName);
		//creating the in memory table as reviews
		df.createOrReplaceTempView("reviews");
		
		//Extracting data in dataset for topStateWiseRestaurants
		Dataset<Row> result = sparkSession
				.sql("SELECT state, COUNT(review_count) as count FROM reviews GROUP BY state ORDER BY count DESC");
		//to view data result on terminal
		result.show();
		//to save the data result in specific location with csv file type
		result.write().option("header", true).csv(outputPath + "/topStateWiseRestaurant");
		
		//Extracting data in dataset for top10Cities
		Dataset<Row> topCities = sparkSession.sql(
				"SELECT city,COUNT(review_count) AS total_reviews FROM reviews GROUP BY city ORDER BY total_reviews DESC limit 10 ");
		//to view data result on terminal
		topCities.show();
		//to save the data result in specific location with csv file type
		topCities.write().option("header", true).csv(outputPath + "/top10Cities");
		
		//Extracting data in dataset for topRestaurantPostCovid
		Dataset<Row> topRestaurants = sparkSession.sql(
				"SELECT name,COUNT(review_count) as total_count, AVG(stars) as avg_stars FROM (SELECT * FROM reviews where stars IS NOT NULL) GROUP BY name ORDER BY total_count desc");
		//to view data result on terminal
		topRestaurants.show();
		//to save the data result in specific location with csv file type
		topRestaurants.write().option("header", true).csv(outputPath + "/topRestaurantPostCovid");
		
		//Extracting data in dataset for ratingCount
		Dataset<Row> ratingCount = sparkSession.sql(
		"SELECT customers_stars, count(customers_stars) FROM reviews GROUP BY customers_stars ORDER BY customers_stars DESC");
		//to view data result on terminal
		ratingCount.show();
		//to save the data result in specific location with csv file type
		ratingCount.write().option("header", true).csv(outputPath + "/ratingCount");
		
		//Extracting data in dataset for Top10usefulRestaurents
		Dataset<Row> topUseful = sparkSession.sql(
				"SELECT name,SUM(useful) AS total_useful FROM reviews GROUP BY name ORDER BY total_useful DESC limit 10 ");
		//to view data result on terminal
		topUseful.show();
		//to save the data result in specific location with csv file type
		topUseful.write().option("header", true).csv(outputPath + "/top10UsefulRestaurents");
		
		//Extracting data in dataset for top10CityOpenRestaurents
		Dataset<Row> openRestaurents = sparkSession.sql(
				"SELECT city,COUNT(is_open) AS total_open FROM reviews where is_open=1 GROUP BY city ORDER BY total_open DESC limit 10 ");
		//to view data result on terminal
		openRestaurents.show();
		//to save the data result in specific location with csv file type
		openRestaurents.write().option("header", true).csv(outputPath + "/top10CityOpenRestaurents");
		
		//Extracting data in dataset for top10CityOpenRestaurents
		Dataset<Row> closeRestaurents = sparkSession.sql(
				"SELECT city,count(*) AS stars FROM reviews WHERE is_open = '0' GROUP BY city ORDER BY stars DESC limit 10 ");
		//to view data result on terminal
		closeRestaurents.show();
		//to save the data result in specific location with csv file type
		closeRestaurents.write().option("header", true).csv(outputPath + "/top10CityCloseRestaurents");
		
		//Extracting data in dataset for top10FunnyRestaurants
		Dataset<Row> topFunny = sparkSession.sql(
				"SELECT name,SUM(funny) AS total_funny FROM reviews GROUP BY name ORDER BY total_funny DESC limit 10 ");
		//to view data result on terminal
		topFunny.show();
		//to save the data result in specific location with csv file type
		topFunny.write().option("header", true).csv(outputPath + "/top10FunnyRestaurents");
		
		//Extracting data in dataset for top10CoolRestaurents
		Dataset<Row> topCool = sparkSession.sql(
				"SELECT name,SUM(cool) AS total_cool FROM reviews GROUP BY name ORDER BY total_cool DESC limit 10 ");
		//to view data result on terminal
		topCool.show();
		//to save the data result in specific location with csv file type
		topCool.write().option("header", true).csv(outputPath + "/top10CoolRestaurents");
		
		//Extracting data in dataset for top10CityIndianRestaurents
		Dataset<Row> indianRestaurents = sparkSession.sql(
				"SELECT city,count(*) AS stars FROM reviews WHERE categories LIKE '%Indian%' GROUP BY city ORDER BY stars DESC limit 10 ");
		//to view data result on terminal
		indianRestaurents.show();
		//to save the data result in specific location with csv file type
		indianRestaurents.write().option("header", true).csv(outputPath + "/top10CityIndianRestaurents");
		
	}

}
