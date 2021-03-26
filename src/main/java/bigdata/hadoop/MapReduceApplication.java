package bigdata.hadoop;

import bigdata.hadoop.data.ScreenAreaCounter;
import bigdata.hadoop.maplogfiles.LogFilesInputFormat;
import bigdata.hadoop.maplogfiles.LogFilesMapper;
import bigdata.hadoop.screenarea.ScreenAreaInputFormat;
import bigdata.hadoop.screenarea.ScreenAreaMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Точка входа. Здесь задаются основные параметры hadoop job.
 * @author Dmitriy Khoroshev
 */
@Slf4j
public class MapReduceApplication {

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("You should specify input, output folders, screen area and temperature reference books!");
        }
        Path outputDirectory = new Path(args[1]);
        Path inputDirectory = new Path(args[0]);
        Path SccreenAreaFile = new Path(args[2]);

        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set("mapreduce.input.textinputformat.separator", ",|-");
        try {
            Job job = Job.getInstance(conf, "Clicks count");
            job.setJarByClass(MapReduceApplication.class);

            //use MultipleOutputs and specify different Record class and Input formats
            MultipleInputs.addInputPath(job, inputDirectory, LogFilesInputFormat.class, LogFilesMapper.class);
            MultipleInputs.addInputPath(job, SccreenAreaFile, ScreenAreaInputFormat.class, ScreenAreaMapper.class);

//            job.setGroupingComparatorClass(MetricsComparator.class);
//
//            job.setSortComparatorClass(MetricsComparator.class);

            job.setReducerClass(ReducerHadoop.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            /* Формат выходного файла */
            job.setOutputFormatClass(TextOutputFormat.class);

            FileOutputFormat.setOutputPath(job, outputDirectory);

            log.info("=====================JOB STARTED=====================");
            try {
                job.waitForCompletion(true);
            }catch (InterruptedException | ClassNotFoundException e) {
                log.error("Error starting job!");
            }
            log.info("=====================JOB ENDED=====================");
            // проверяем статистику по счётчикам
            Counter counter = job.getCounters().findCounter(ScreenAreaCounter.MALFORMED);
            log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
        } catch (IOException e) {
            log.error("Error job configuration");
        }
    }
}
