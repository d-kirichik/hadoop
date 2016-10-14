import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by seven-teen on 14.10.16.
 */
public class Main {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("Simple Sort");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SimpleMapper.class);
        job.setPartitionerClass(SimplePartitioner.class);
        job.setGroupingComparatorClass(SimpleComparator.class);
        job.setReducerClass(SimpleReducer.class);

        job.setOutputKeyClass(CSVInput.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
