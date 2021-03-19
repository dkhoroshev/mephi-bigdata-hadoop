package bigdata.hadoop;

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

public class ScreenArea implements Writable {
    private String value;
    private int additional;

    public ScreenArea() {
        this.value = "TEST";
        this.additional = 0;
    }

    public ScreenArea(String val, int addi) {
        this.value = val;
        this.additional = addi;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (null == in) {
            throw new IllegalArgumentException("in cannot be null");
        }
        String value = in.readUTF();
        int addi = in.readInt();
        this.value = value.trim();
        this.additional = addi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (null == out) {
            throw new IllegalArgumentException("out cannot be null");
        }
        out.writeUTF(this.value);
        out.writeInt(this.additional);
    }
    @Override
    public String toString() {
        return "ScreenArea" + value + "t" + additional;
    }
}
