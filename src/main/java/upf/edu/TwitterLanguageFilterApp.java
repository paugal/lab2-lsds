package upf.edu;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import upf.edu.parser.SimplifiedTweet;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.io.FileWriter;
import java.io.IOException;

public class TwitterLanguageFilterApp {
    public static void main(String[] args) throws IOException {
        List<String> argsList = Arrays.asList(args);
        String lang = argsList.get(0);
        String outputDir = argsList.get(1);
        String input = argsList.get(2);

        long start = System.currentTimeMillis();

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Filter");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sparkContext.textFile(input)
            .filter(sentence -> !sentence.isEmpty())
            .map(word -> SimplifiedTweet.fromJson(word)) // Get Optional from tweet json
            .filter(s-> s.isPresent()) // Filter only present
            .map(s -> s.get()) // Optional to SimplifiedTweet
            .filter(s -> s.getLanguage().equals(lang)) // Filter language
            .map(s -> s.toString()); // SimplifiedTweet to String
		sentences.saveAsTextFile(outputDir);

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        File file = new File(outputDir+"/results","results.txt");
        file.getParentFile().mkdirs();
        file.createNewFile();
        FileWriter fw = new FileWriter(file);
        fw.write("Time to process tweets in "+lang+" is "+ Long.toString(elapsedTime) + "ms");
        fw.close();
        System.out.println("Time to process tweets in "+lang+" is "+ Long.toString(elapsedTime) + "ms");
    }
}


