package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class app2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("src/main/java/ma/enset/words.txt");
        JavaRDD<String> rdd2 = rdd1.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String,Integer> pairRDD = rdd2.flatMapToPair(x -> Arrays.asList(new scala.Tuple2<>(x,1)).iterator());
        JavaPairRDD<String,Integer> res = pairRDD.reduceByKey((x,y) -> x+y);
        res.collect().forEach(System.out::println);
    }
}
