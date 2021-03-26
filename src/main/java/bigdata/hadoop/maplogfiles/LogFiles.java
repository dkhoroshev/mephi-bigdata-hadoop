package bigdata.hadoop.maplogfiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogFiles implements Writable {
    private String value;

    public LogFiles() {
        this.value = "TEST";
    }

    public LogFiles(String val) {
        this.value = val;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (null == in) {
            throw new IllegalArgumentException("in cannot be null");
        }
        String value = in.readUTF();
        this.value = value.trim();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (null == out) {
            throw new IllegalArgumentException("out cannot be null");
        }
        out.writeUTF(this.value);
    }

    @Override
    public String toString() {
        return "LogFiles" + value;
    }
}
