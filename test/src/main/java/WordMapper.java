import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by seven-teen on 17.09.16.
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] words = line.split(" ");
        for(String word : words){
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
       /* <получаем строку из value, удаляем все спецсимволы, переводим в lowercase, разбиваем на слова и каждое слово пишем в контекст с счетчиком 1
        в контекст пишется пара — Text и IntWritable >*/
    }
}