
import bigdata.hadoop.ReducerHadoop;
import bigdata.hadoop.maplogfiles.LogFilesMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;


    private final String testreducer = "600,532,1200,798-area_6";
    private final String testreducer2 = "display";
    private final String testmapper = "881,687,user3,176476791";




    @Before
    public void setUp() throws IOException {
        LogFilesMapper mapper = new LogFilesMapper();
        ReducerHadoop reducer = new ReducerHadoop();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }


    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testmapper))
                .withOutput(new Text(testreducer2), new LongWritable(1))
                .runTest();
    }


    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(1));
        values.add(new LongWritable(1));
        reduceDriver
                .withInput(new Text(testreducer2), values)
                .withOutput(new Text(testreducer2), new LongWritable(2))
                .runTest();
    }


    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testreducer))
                .withInput(new LongWritable(), new Text(testreducer))
                .withOutput(new Text(testreducer2), new LongWritable(2))
                .runTest();
    }

}
