package bigdata.hadoop;

import bigdata.hadoop.data.ScreenAreaWritable;
import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

@Slf4j
public class ReducerHadoop extends Reducer<ScreenAreaWritable, IntWritable, ScreenAreaWritable, IntWritable>{

    public void reduce(ScreenAreaWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        log.info("-----------Start reducer-----------");

        long sum = 0;

        for (IntWritable val:values) {
            sum += val.get();
        }

        context.write(key, new IntWritable((int) sum));
    }
}
