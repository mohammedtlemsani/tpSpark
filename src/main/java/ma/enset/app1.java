package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.Seq;
import scala.jdk.CollectionConverters;

import java.util.Arrays;
import java.util.List;

public class app1 {
    public static  void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(new Integer[]{12,10,16,9,8,0});
        JavaRDD<Integer> rdd1 = sc.parallelize(list);
        JavaRDD<Integer> rdd2 = rdd1.map(x -> x*x);
        List<Integer> res = rdd2.collect();
        res.forEach(System.out::println);
    }
}