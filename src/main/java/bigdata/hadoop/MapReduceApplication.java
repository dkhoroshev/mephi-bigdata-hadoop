package bigdata.hadoop;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.SetFile.Reader;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Точка входа. Здесь задаются основные параметры hadoop job.
 * @author Dmitriy Khoroshev
 */
@Log4j
public class MapReduceApplication {
    public static void main(String[] args) {
        if (args.length < 4) {
            throw new RuntimeException("You should specify input, output folders, screen area and temperature reference books!");
        }

        String TemperatureFile = new String("hdfs://localhost:9000/" + args[2]);
        String SccreenAreaFile = new String("hdfs://localhost:9000/" + args[3]);

        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set("mapreduce.input.textinputformat.separator", ",");
        try {
            Job job = Job.getInstance(conf, "Clicks count");
            job.setJarByClass(MapReduceApplication.class);
            job.setMapperClass(MapperHadoop.class);
            job.setReducerClass(ReducerHadoop.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            try{
                job.addCacheFile(new URI(TemperatureFile));
                job.addCacheFile(new URI(SccreenAreaFile));
            }catch (URISyntaxException e) {
                log.fatal("Error job dictionary URI");
            }

            Path outputDirectory = new Path(args[1]);
            Path inputDirectory = new Path(args[0]);

            FileInputFormat.addInputPath(job, inputDirectory);
            FileOutputFormat.setOutputPath(job, outputDirectory);

            log.info("=====================JOB STARTED=====================");
            try {
                job.waitForCompletion(true);
            }catch (InterruptedException | ClassNotFoundException e) {
                log.fatal("Error starting job!");
            }
            log.info("=====================JOB ENDED=====================");
            // проверяем статистику по счётчикам
            Counter counter = job.getCounters().findCounter(CounterType.MALFORMED);
            log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
        } catch (IOException e) {
            log.fatal("Error job configuration");
        }
    }
}
