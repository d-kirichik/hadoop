package iu9.trident.lab8;

import com.google.common.io.Files;
import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by seven-teen on 22.12.16.
 */

public class FlightDataSpout implements IBatchSpout {
    private File curDir;
    private Fields fields;
    private HashMap<Long, List<String>> batches;
    private static int posInDir;

    public FlightDataSpout(Fields fields) {
        this.fields = fields;
        posInDir = 0;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        curDir = new File("/home/seven-teen/Documents/P_P/lab8/input_data/");
        batches = new HashMap<>();
    }

    @Override
    public void emitBatch(long id, TridentCollector collector) {
        List<String> batch = batches.computeIfAbsent(id, k -> readNextFile());
        if (batch != null) {
            batch.forEach(s -> collector.emit(new Values(s)));
        }
    }

    private List<String> readNextFile() {
        File[] listFiles = curDir.listFiles();
        if (listFiles == null || listFiles.length == 0) {
            return null;
        }

        File current = listFiles[posInDir];
        if (!current.isFile() || !current.canRead()) {
            posInDir++;
            return readNextFile();
        }

        List<String> lines;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(current), Charsets.UTF_8))) {
            lines = br.lines().collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            posInDir++;
            return readNextFile();
        }

        try {
            Files.move(current, new File("/home/seven-teen/Documents/P_P/lab8/output_data/" + current.getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            posInDir++;
        }

        return lines;
    }

    @Override
    public void ack(long id) { }

    @Override
    public void close() { }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }


}