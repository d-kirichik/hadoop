package lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by seven-teen on 16.11.16.
 */
public class Main {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("Flight Data join");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightDataDelayMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportDataNameMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(FlightDataJoinPartitioner.class);
        job.setGroupingComparatorClass(FlightDataJoinComparator.class);
        job.setReducerClass(FlightDataJoinReducer.class);
        job.setMapOutputKeyClass(FlightAirportJoinerKey.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
