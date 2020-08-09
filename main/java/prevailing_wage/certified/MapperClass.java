package prevailing_wage.certified;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import prevailing_wage.AverageCounter;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, AverageCounter> {
    Text year = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String s = line.replaceAll(", ", " ");
        String[] values = s.split(",");
        if (values.length == 11){
            year.set(values[7]);
            if (values[1].replaceAll("\"","").equals("CERTIFIED")) {
                try {
                    AverageCounter counter = new AverageCounter(1, Double.parseDouble(values[6]));
                    context.write(year, counter);
                } catch (Exception e) {
                    //do nothing
                }
            }
        }
    }
}
