package bigdata.hadoop.screenarea;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class ScreenAreaReader extends RecordReader<Text, ScreenArea> {
    private LineRecordReader lineRecordReader = null;
    private Text key = null;
    private ScreenArea valueSecondClass = null;

    @Override
    public void close() throws IOException {
        if (null != lineRecordReader) {
            lineRecordReader.close();
            lineRecordReader = null;
        }
        key = null;
        valueSecondClass = null;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public ScreenArea getCurrentValue() throws IOException, InterruptedException {
        return valueSecondClass;
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
            valueSecondClass = null;
            return false;
        }

        // otherwise, take the line and parse it
        Text line = lineRecordReader.getCurrentValue();

        String str = line.toString();

        System.out.println("ScreenArea:" + str);
        String[] arr = str.split(",|-");
        int addi = Integer.parseInt(arr[2]);

        key = new Text(arr[0].trim());
        valueSecondClass = new ScreenArea(arr[1].trim(), addi);

        return true;
    }
}
