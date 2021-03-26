package bigdata.hadoop.maplogfiles;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogFilesMapper extends Mapper<Text, LogFiles, Text, Text> {

    public void map(Text key, LogFiles value, Context context) throws IOException, InterruptedException {
        System.out.println("FirstMap:" + key.toString() + " " + value.toString());
        context.write(key, new Text(value.toString()));
    }
}
