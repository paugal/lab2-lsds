package upf.edu;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import upf.edu.parser.ExtendedSimplifiedTweet;

import java.io.BufferedWriter;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.*;
import java.io.FileWriter;
import java.io.IOException;

//spark-submit --class upf.edu.BiGramsApp --master local[*] target/lab2-4-1.0-SNAPSHOT.jar es ./dataset/out/ ./dataset/Eurovision9.json

public class BiGramsApp {
    public static void main(String[] args) throws IOException {
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String inputFile = argsList.get(2);
        SparkConf conf = new SparkConf().setAppName("TwitterLanguageFilterApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sparkContext.textFile(inputFile).filter(x -> !x.isEmpty());
        JavaRDD<String> tweets = sentences.map(word -> ExtendedSimplifiedTweet.fromJson(word))
                .filter(o -> o.isPresent()) // filter para tener solo los que tienen algo del optional
                .map(o -> o.get()) // pasar de optional a SimplifiedTweet
                .filter(o -> o.getIsRetweeted() == false).filter(o -> o.getLanguage().equals(language)) // filtrar idioma
                .map(o -> o.getText().replaceAll("https?://\\S+\\s?", "").replaceAll("@\\S+\\s?", "")
                        .replaceAll("#\\S+\\s?", "").replaceAll("[,.!¡¿?:;…-]", "").trim().toLowerCase())
                .map(o -> biGram(o)).filter(x -> !x.isEmpty()).flatMap(s -> Arrays.asList(s.split("\\s")).iterator());

        JavaPairRDD<String, Integer> biGram = tweets.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> swappedbiGram = biGram
                .flatMapToPair(x -> Collections.singletonList(x.swap()).iterator()).sortByKey(false); // invertimos orden para ordenar por key

        List<Tuple2<Integer, String>> biGramList = swappedbiGram.take(10);

        biGram.saveAsTextFile(outputFile);

        sparkContext.close();

        String finalTop10 = "";
        for (int i = 0; i < biGramList.size(); i++) {
            finalTop10 = finalTop10 + biGramList.get(i) + "\n";
        }
        File file = new File(outputFile+"/results","results.txt");
        file.getParentFile().mkdirs();
        file.createNewFile();
        FileWriter fw = new FileWriter(file);
        fw.write(finalTop10);
        fw.close();
        System.out.println(finalTop10);
    }

    public static String biGram(String text) {

        String finalString = "";

        List<String> aux = Arrays.asList(text.split("\\s"));
        for (int i = 0; i < aux.size() - 1; i++) {
            if ((!aux.get(i).isEmpty() && !aux.get(i + 1).isEmpty())) { // para que solo coja bigrams y no si hay algun intro como linea

                finalString = finalString + "(" + aux.get(i) + "," + aux.get(i + 1) + ") ";

            }
        }

        return finalString;

    }

}