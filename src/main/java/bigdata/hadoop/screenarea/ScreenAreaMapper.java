package bigdata.hadoop.screenarea;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScreenAreaMapper extends Mapper<Text, ScreenArea, Text, Text> {

    public void map(Text key, ScreenArea value, Context context) throws IOException, InterruptedException {
        System.out.println("ScreenAreaMap:" + key.toString() + " " + value.toString());
        context.write(key, new Text(value.toString()));
    }
}
