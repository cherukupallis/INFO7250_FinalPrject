package top_occupation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class ReducerClass extends Reducer<Text, IntWritable,Text,IntWritable> {

    TreeMap<Integer, String> treeMap;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        treeMap = new TreeMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value :values)
            count+= value.get();

        treeMap.put(count,key.toString());

        if (treeMap.size() > 10)
            treeMap.remove(treeMap.firstKey());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> entry : treeMap.entrySet())
        {
            int count = entry.getKey();
            String occupation = entry.getValue();
            context.write(new Text(occupation), new IntWritable(count));
        }
    }
}
