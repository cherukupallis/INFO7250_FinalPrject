package case_status;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, IntWritable>{
    Text status = new Text();
    IntWritable ONE = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String s = line.replaceAll(", ", " ");
            String[] values = s.split(",");
            if (values.length == 11){
                status.set(values[1]);
                context.write(status,ONE);
        }
    }
}
