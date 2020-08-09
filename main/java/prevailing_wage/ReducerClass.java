package prevailing_wage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, AverageCounter,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<AverageCounter> values, Context context) throws IOException, InterruptedException {
        int count =0;
        double sum =0;
        for (AverageCounter value : values){
            try{
                count += value.getCount();
                sum += value.getSum();
            }catch (Exception e){
                count+=0;
                sum+=0;
            }
        }
        double avg = sum/count;
        context.write(key, new Text(String.valueOf(avg)));
    }
}
