import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by seven-teen on 14.10.16.
 */
public class SimpleReducer extends Reducer<CSVInput, Text, CSVInput, Text>{
    @Override
    protected void reduce(CSVInput key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
        for(Text v : vals)
            context.write(key, v);
    }
}
