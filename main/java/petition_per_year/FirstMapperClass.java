package petition_per_year;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FirstMapperClass extends Mapper<LongWritable, Text,Text, IntWritable>{
    Text year = new Text();
    IntWritable ONE = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String s = line.replaceAll(", ", " ");
        String[] values = s.split(",");
        if (values.length == 11){
            year.set(values[7]);
            context.write(year,ONE);
        }
    }
}
