import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by seven-teen on 14.10.16.
 */
public class SimpleMapper extends Mapper<LongWritable, Text, CSVInput, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            CSVInput in = new CSVInput(value.toString());
            if (!(in.cancelled) || in.arr_delay <= 0)
                context.write(in, value);
    }
}
