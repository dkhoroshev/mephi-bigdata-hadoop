package bigdata.hadoop.maplogfiles;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LogFilesInputFormat extends FileInputFormat<Text, LogFiles>{
    @Override
    public RecordReader<Text, LogFiles> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new LogFilesReader();
    }
}
