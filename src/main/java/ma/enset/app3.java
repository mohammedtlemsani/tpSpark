package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class app3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.textFile("src/main/java/ma/enset/ventes_large.txt")
                .mapToPair(x -> new scala.Tuple2<String,Integer>(x.split(" ")[1],new Integer(x.split(" ")[3]))).reduceByKey((x,y) -> x+y).collect().forEach(System.out::println);
    }
}
