
import bigdata.hadoop.ReducerHadoop;
import bigdata.hadoop.maplogfiles.LogFilesMapper;
import bigdata.hadoop.data.ScreenAreaWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, ScreenAreaWritable, IntWritable> mapDriver;
    private ReduceDriver<ScreenAreaWritable, IntWritable, ScreenAreaWritable, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, ScreenAreaWritable, IntWritable, ScreenAreaWritable, IntWritable> mapReduceDriver;


    private final String testreducer = "600,532,1200,798-area_6";
    private final String testreducer2 = "display";
    private final String testmapper = "881,687,user3,176476791";




    @BeforeAll
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
                .withOutput(new ScreenAreaWritable(testreducer2), new IntWritable(1))
                .runTest();
    }


    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new ScreenAreaWritable(testreducer2), values)
                .withOutput(new ScreenAreaWritable(testreducer2), new IntWritable(2))
                .runTest();
    }


    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testmapper))
                .withInput(new LongWritable(), new Text(testmapper))
                .withOutput(new ScreenAreaWritable(testreducer2), new IntWritable(2))
                .runTest();
    }

}
