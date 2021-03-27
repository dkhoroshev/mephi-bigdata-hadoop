package bigdata.hadoop.data;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@EqualsAndHashCode
public class ScreenAreaWritable implements WritableComparable<ScreenAreaWritable> {

    private String severityWithScreenArea;

    public ScreenAreaWritable() { }

    public ScreenAreaWritable(String severityWithScreenArea) {
        this.severityWithScreenArea = severityWithScreenArea;
    }

    public ScreenAreaWritable prettify() {
        String[] keys = this.getSeverityWithScreenArea().split(" ");
        return new ScreenAreaWritable(keys[0]);
    }

    @Override
    public String toString() {
        return this.severityWithScreenArea;
    }

    @Override
    public int compareTo(ScreenAreaWritable o) {
        return StringUtils.compare(this.severityWithScreenArea, o.getSeverityWithScreenArea());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(severityWithScreenArea);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.severityWithScreenArea = in.readLine();
    }
}
