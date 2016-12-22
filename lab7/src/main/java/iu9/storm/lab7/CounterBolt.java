package iu9.storm.lab7;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by seven-teen on 22.12.16.
 */

public class CounterBolt extends BaseRichBolt {

    private Map<String,Integer> counter;

    private static void printToFile(String key, Integer value) {
        try(FileWriter writer = new FileWriter("/home/seven-teen/Documents/P_P/lab7/output_data/output_lab7.txt", false)){
            writer.write(key + " : " + value);
            writer.flush();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector output) {
        counter = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("sync")) {
            counter.forEach(CounterBolt::printToFile);
            counter.clear();
        }
        else {
            String word = input.getStringByField("word").toLowerCase();
            int v = counter.getOrDefault(word, 0);
            counter.put(word, v + 1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}