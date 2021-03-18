package bigdata.hadoop;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

@Log4j
public class ReducerHadoop extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context){
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        try {
            context.write(key, new IntWritable(sum));
        }catch (IOException | InterruptedException e){
            log.error("Error write reduced data.");
        }
    }
}
