package com.mr.matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Matrix A (i*j)
 * Matrix B (i*j)
 * <p>
 * A + B
 * <p>
 * Result Matrix after addition (i*j)
 * <p>
 * matrix A represented as
 * A i k val
 * A 0 0 2
 * A 0 1 5
 * A 1 0 3
 * A 1 1 2
 * <p>
 * matrix B represented as
 * B i k val
 * B 0 0 1
 * B 0 1 12
 * B 1 0 13
 * B 1 1 4
 * <p>
 * Result Matrix will be printed as
 * <p>
 * i j Val
 * 0 0 __
 */
public class MatrixAddition {
    private static final String TAB = "\\t";

    private class MtxAddMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) {
            Configuration conf = context.getConfiguration();
            int i = Integer.parseInt(conf.get("i"));
            int j = Integer.parseInt(conf.get("j"));
            String line = value.toString();
            String[] indicesAndValue = line.split(TAB);
            Text outputKey = new Text();
            Text outputValue = new Text();
            outputKey.set(indicesAndValue[1] + TAB + indicesAndValue[2]);
            outputValue.set(indicesAndValue[3]);
        }
    }

    private class MtxAddReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text outputValue = new Text();
            double result = 0.0;
            for (Text val : values) {
                result += Double.parseDouble(val.toString());
            }
            outputValue.set(Double.toString(result));
            context.write(key, outputValue);

        }
    }
}
