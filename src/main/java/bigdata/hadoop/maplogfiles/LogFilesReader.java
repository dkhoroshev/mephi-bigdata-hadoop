package bigdata.hadoop.maplogfiles;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class LogFilesReader extends RecordReader<Text, LogFiles> {
    private LineRecordReader lineRecordReader = null;
    private Text key = null;
    private LogFiles valueFirstClass = null;

    @Override
    public void close() throws IOException {
        if (null != lineRecordReader) {
            lineRecordReader.close();
            lineRecordReader = null;
        }
        key = null;
        valueFirstClass = null;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public LogFiles getCurrentValue() throws IOException, InterruptedException {
        return valueFirstClass;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        close();
        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            key = null;
            valueFirstClass = null;
            return false;
        }

        // otherwise, take the line and parse it
        Text line = lineRecordReader.getCurrentValue();
        String str = line.toString();
        System.out.println("FirstClass:" + str);
        String[] arr = str.split(",|-");
        key = new Text(arr[0].trim());
        valueFirstClass = new LogFiles(arr[1].trim());

        return true;
    }
}
