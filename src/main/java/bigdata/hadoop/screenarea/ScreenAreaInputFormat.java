package bigdata.hadoop.screenarea;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ScreenAreaInputFormat extends FileInputFormat<Text, ScreenArea> {
    @Override
    public RecordReader<Text, ScreenArea> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new ScreenAreaReader();
    }
}
