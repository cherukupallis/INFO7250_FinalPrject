package top_employers_per_year;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ReducerClass extends Reducer<Text, Text,Text,Text> {
    private List<TreeMap<Integer,String>> treeMapList;
    @Override
    public void setup(Context context) {
        treeMapList = new ArrayList<>();
        for (int i=0 ;i < 6; i ++){
            TreeMap<Integer, String> year = new TreeMap<>();
            treeMapList.add(year);
        }
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] count = new int[6];
        String[] years = new String[]{"2011","2012","2013","2014","2015","2016"};
        for (Text value : values){
            String year = value.toString();
            switch (year){
                case "2011":{
                    count[0]++;
                    break;
                }
                case "2012":{
                    count[1]++;
                    break;
                }
                case "2013":{
                    count[2]++;
                    break;
                }
                case "2014":{
                    count[3]++;
                    break;
                }
                case "2015":{
                    count[4]++;
                    break;
                }
                case "2016": {
                    count[5]++;
                    break;
                }
            }
        }

        for (int i = 0 ; i < 6 ; i++){
            treeMapList.get(i).put(count[i],years[i]+" : "+key.toString());
            if (treeMapList.get(i).size()>10){
                treeMapList.get(i).remove(treeMapList.get(i).firstKey());
            }
        }
    }


    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {

        for (TreeMap<Integer,String> map : treeMapList){
            for (Map.Entry<Integer, String> entry : map.entrySet())
            {
                int count = entry.getKey();
                String name = entry.getValue();
                context.write(new Text(name), new Text(String.valueOf(count)));
            }
        }
    }
}
