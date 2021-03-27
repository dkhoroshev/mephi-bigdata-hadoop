package bigdata.hadoop.maplogfiles;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import bigdata.hadoop.data.ScreenAreaCounter;
import bigdata.hadoop.data.ScreenAreaWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class LogFilesMapper extends Mapper<LongWritable, Text, ScreenAreaWritable, IntWritable> {

    List<String> dictPlace = new ArrayList<>();
    private final Pattern dictpattern = Pattern.compile("(\\d+),(\\d+),(\\d+),(\\d+)-(\\S+)");
    private final Pattern logpattern = Pattern.compile("(\\d+),(\\d+),.*$");
    private final IntWritable INCR = new IntWritable(1);

    /**
     * Метод setup используется для считывания инфо из справочника
     * и записи данных в List<String> dictPlace.
     *
     * @param context данные конфиrа
     * @throws IOException при исключении ввода
     */

    public void setup(Context context) {
        log.info("-----------Mapper setup-----------");
        Configuration conf = context.getConfiguration();
        if (conf.get("ScreenAreaFile") != null) {
            try {
                FileSystem fs = FileSystem.get(conf);
                Path screenareadict = new Path(conf.get("ScreenAreaFile"));
                if (!fs.exists(screenareadict)) {
                    log.error("Dict file not found");
                }
                // open and read from file
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(screenareadict)));
                String line = null;
                Matcher dictmather;
                while ((line = reader.readLine()) != null) {
                    dictmather = dictpattern.matcher(line);
                    while (dictmather.find()) {
                        dictPlace.add(line); //add the line to ArrayList
                    }
                }
            } catch (Exception e) {
                log.error("Unable to read ScreenArea dictionary", e);
            }
        } else {
            dictPlace.add("0,0,3000,3000-display");
        }
    }
    /**
     * Метод мап является ключевым, в нем происходит считывания инфы с исходника,
     * считывания инфы из справочника для опредления области клика и записи в контекст
     * название области и факт клика.
     *
     * @param key ключ
     * @param value значение
     * @param context данные конфиrа
     * @throws IOException при исключении ввода
     * @throws InterruptedException при исключении ввода
     */

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        ScreenAreaWritable result = new ScreenAreaWritable();
        result.setDict(dictPlace);

        log.info("-----------Start mapper-----------");
        Matcher logmatcher = logpattern.matcher(value.toString());
        while (logmatcher.find()) {
            result.setSeverityWithScreenArea(logmatcher.group(1)
                    + " "
                    + logmatcher.group(2));
        }

        result.CompareXY();
        if (result.getSeverityWithScreenArea() != null) {
            context.write(result, INCR);
        } else {
            context.getCounter(ScreenAreaCounter.PARSE_ERROR).increment(1);
        }
    }
}
