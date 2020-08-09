package petition_per_year;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstReducerClass extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int summation = 0;
        for (IntWritable value : values){
            summation += value.get();
        }
        context.write(key,new IntWritable(summation));
    }
}
