package iu9.storm.lab7;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by seven-teen on 22.12.16.
 */
public class SplitterBolt  extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector output) {
        this.collector = output;
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("execute split");
        String [] words = input.getStringByField("line").split("[^a-zA-Z0-9]+");
        for (String word : words) {
            word = word.toLowerCase();
            collector.emit(input,new Values(word));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}