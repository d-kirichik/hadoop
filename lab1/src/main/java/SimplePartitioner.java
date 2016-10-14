import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by seven-teen on 14.10.16.
 */
public class SimplePartitioner extends Partitioner<CSVInput, Text> {
    @Override
    public int getPartition(CSVInput csvInput, Text text, int i) {
        return ((int)csvInput.arr_delay & Integer.MAX_VALUE) % i;
    }
}
