package bigdata.hadoop;

import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

@Slf4j
public class ReducerHadoop extends Reducer<Text, LongWritable, Text, LongWritable>{

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        log.info("-----------Start reducer-----------");

        long sum = 0;

        for (LongWritable val:values) {
            sum += val.get();
        }

        context.write(key, new LongWritable(sum));
    }
}
