package top_occupation.peryear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class ReducerClass extends Reducer<Text, IntWritable,Text,IntWritable> {

    HashMap<String, TreeMap<Integer,String>> hashMap;
    @Override
    protected void setup(Context context) {
        hashMap = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;

        for (IntWritable value: values)
            count += value.get();

        String year = key.toString().split(":")[0];
        if (hashMap.get(year)!= null) {
            TreeMap<Integer,String> treeMap = hashMap.get(year);
            if (treeMap.size() >10 ){
                treeMap.remove(treeMap.firstKey());
            }
            treeMap.put(count,key.toString());
            hashMap.put(year,treeMap);
        }else{
            TreeMap<Integer,String> treeMap = new TreeMap<>();
            treeMap.put(count,key.toString());
            hashMap.put(year,treeMap);
        }

//        context.write(key,new IntWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(TreeMap<Integer,String> map : hashMap.values()){
            for (Map.Entry<Integer, String> entry : map.entrySet())
            {
                int count = entry.getKey();
                String occupation = entry.getValue();
                context.write(new Text(occupation), new IntWritable(count));
            }
        }
    }
}
