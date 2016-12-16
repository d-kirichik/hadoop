package lab5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by seven-teen on 16.12.16.
 */
public class Main {

    private static final short ORIGIN_AIRPORT = 11;
    private static final short DEST_AIRPORT = 14;
    private static final short ARR_DELAY_NEW = 18;
    private static final short CANCELLED = 19;



    private static Tuple2<Integer, String> parseAirportData(String input){
        String [] tmp = input.split(",");
        return new Tuple2<>(Integer.parseInt(tmp[0].substring(1, tmp[0].length() - 1)), tmp[1] + '\"');
    }

    private static Tuple2<Tuple2<Integer, Integer>, FlightSerializable> parseFlightData(String input){
        String [] tmp = input.split(",");
        FlightSerializable flight = new FlightSerializable(Integer.parseInt(tmp[ORIGIN_AIRPORT]),
                Integer.parseInt(tmp[DEST_AIRPORT]), tmp[ARR_DELAY_NEW].isEmpty() ? 0f : Float.parseFloat(tmp[ARR_DELAY_NEW]), Boolean.parseBoolean(tmp[CANCELLED]));
        Tuple2 <Integer, Integer> key = new Tuple2<>(Integer.parseInt(tmp[ORIGIN_AIRPORT]),
                Integer.parseInt(tmp[DEST_AIRPORT]));
        return new Tuple2<>(key, flight);
    }

    private static String formOutputData(Tuple2 <Integer, Integer> id, Statistics s, Map<Integer, String> airports){
        String originAirport = airports.get(id._1());
        String destAirport = airports.get(id._2());
        String stats = s.toString();
        return "From: " + originAirport + " To: " + destAirport + ' ' + stats;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab5");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flightInput = sc.textFile("sample.csv").filter(s -> (!s.isEmpty() && !s.contains("YEAR")));
        JavaRDD<String> airportInput = sc.textFile("airports.csv").filter(s -> !s.contains("Code"));

        JavaPairRDD<Integer, String> airportData = airportInput.mapToPair(Main::parseAirportData);
        JavaPairRDD<Tuple2<Integer,Integer>,FlightSerializable> flightData = flightInput.mapToPair(Main::parseFlightData);

        JavaPairRDD<Tuple2<Integer, Integer>, Statistics> statistics  = flightData.aggregateByKey(new Statistics(), Statistics::addFlight, Statistics::mergeStatistics);

        final Broadcast<Map<Integer,String>> airportsBroadcasted = sc.broadcast(airportData.collectAsMap());
        JavaRDD<String> statsData = statistics.map(a -> {
            Map<Integer,String> airports = airportsBroadcasted.value();
            return formOutputData(a._1(), a._2(), airports);});
        statsData.saveAsTextFile("hdfs://localhost:9000/user/seven-teen/output_lab5");
        sc.close();
    }
}
