package top_employers_per_year;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, Text> {
    Text year = new Text();
    Text employer = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String s = line.replaceAll(", ", " ");
        String[] values = s.split(",");
        if (values.length == 11){
            year.set(values[7]);
            employer.set(values[2].replaceAll("\"",""));
            context.write(employer,year);
        }
    }
}
