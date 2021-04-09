package bigdata.hadoop.data;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custome type class for mapper and reducer
 */
@Data
@EqualsAndHashCode
public class ScreenAreaWritable implements WritableComparable<ScreenAreaWritable> {

    private String severityWithScreenArea;
    private List<String> dict;
    Integer resultX = null, resultY = null;
    Integer minX = null, maxX = null, minY = null, maxY = null;
    String AreaName = null;

    private final Pattern dictpattern = Pattern.compile("(\\d+),(\\d+),(\\d+),(\\d+)-(\\S+)");

    public ScreenAreaWritable() {}

    public ScreenAreaWritable(String severityWithScreenArea) {
        this.severityWithScreenArea = severityWithScreenArea;
    }

    public ScreenAreaWritable(List<String> dict) {
        this.dict = dict;
    }

    public ScreenAreaWritable prettify() {
        String[] keys = this.getSeverityWithScreenArea().split(" ");
        return new ScreenAreaWritable(keys[0]);
    }

    /**
     * Метод соотносит текущие координаты со словарем,
     * на выходе остается только название области экрана.
     * В случае если данные не соответствуют критерию,
     * добавляется значение к счетчику PARSE_ERROR.
     */
    public void CompareXY() {
        String[] keys = this.getSeverityWithScreenArea().split(" ");
        resultX = Integer.parseInt(keys[0]);
        resultY = Integer.parseInt(keys[1]);

        for (String dictWords : this.dict) {
            Matcher dictmather = dictpattern.matcher(dictWords);
            while (dictmather.find()){
                AreaName = dictmather.group(5);
                minX = Integer.parseInt(dictmather.group(1));
                minY = Integer.parseInt(dictmather.group(2));
                maxX = Integer.parseInt(dictmather.group(3));
                maxY = Integer.parseInt(dictmather.group(4));
            }
            if (resultX >= minX && resultX <= maxX && resultY >= minY && resultY <= maxY) {
                this.severityWithScreenArea = AreaName;
                break;
            } else {
                this.severityWithScreenArea = null;
            }
        }
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
