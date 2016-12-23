package iu9.trident.lab8;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

/**
 * Created by seven-teen on 22.12.16.
 */
public class Main {
    public static void main(String[] args) {
        FlightDataSpout spout = new FlightDataSpout(new Fields("line"));

        TridentTopology topology = new TridentTopology();
        topology.newStream("flightsData", spout)
                .each(new Fields("line"), new ParseFlightFunction(), new Fields("day", "arrDelay", "cancelled"))
                .filter(new Fields("day", "arrDelay", "cancelled"), new ArrDelayFilter())
                .partitionBy(new Fields("day"))
                .partitionAggregate(new Fields("day"), new DayAggregator(), new Fields("stats"))
                .parallelismHint(6)
                .each(new Fields("stats"), new PrintStatsFunction(), new Fields());

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology", conf, topology.build());
    }
}