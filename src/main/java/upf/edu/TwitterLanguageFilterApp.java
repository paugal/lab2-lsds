package upf.edu;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;

public class TwitterLanguageFilterApp {
    public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);
        String lang = argsList.get(0);
        String outputDir = argsList.get(1);
        String input= argsList.get(2);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Filter");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);
        JavaPairRDD<String, Integer> counts = sentences
                .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile(outputDir);

        /*JavaRDD<String> sentences = sparkContext.textFile(input).filter(x -> !x.isEmpty());
		JavaRDD<String> tweets = sentences.map(word -> SimplifiedTweet.fromJson(word)).filter(s-> s.isPresent()) // filter para tener solo los que tienen algo del optional
				.map(s -> s.get()) // pasa de optional a SimplifiedTweet
				.filter(s -> s.getLanguage().equals(lang)) // filtrar idioma
				.map(s -> s.toString()); // este nos pasa de SimplifiedTweet a String
		tweets.saveAsTextFile(outputDir);*/
    }
}
