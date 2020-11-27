package com.mr.matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Matrix A (i*k)
 * Matrix B (k*j)
 * <p>
 * A * B
 * <p>
 * Result Matrix after multiplication (i*j)
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
public class MatrixMultiplication {
    private static final String TAB = "\\t";

    private class MtxMultiMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int i = Integer.parseInt(conf.get("i"));
            int j = Integer.parseInt(conf.get("j"));
            String line = value.toString();
            String[] indicesAndValue = line.split(TAB);
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("A")) {
                for (int k = 0; k < j; k++) {
                    outputKey.set(indicesAndValue[1] + TAB + k); // row k times
                    outputValue.set(indicesAndValue[0] + TAB + indicesAndValue[2]
                            + TAB + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            } else { // B matrix
                for (int k = 0; k < i; k++) {
                    outputKey.set(i + TAB + indicesAndValue[2]); // column k times
                    outputValue.set(indicesAndValue[0] + TAB + indicesAndValue[1]
                            + TAB + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    private class MtxMultiReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text outputValue = new Text();
            Map<Integer, Double> hashA = new HashMap<>();
            Map<Integer, Double> hashB = new HashMap<>();
            for (Text val : values) {
                String[] value = val.toString().split(TAB);
                if (value[0].equals("A")) {
                    hashA.put(Integer.parseInt(value[1]), Double.parseDouble(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Double.parseDouble(value[2]));
                }
            }
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            double result = 0.0;
            double a_ik;
            double b_kj;
            for (int i = 0; i < k; i++) {
                a_ik = hashA.getOrDefault(i, 0.0);
                b_kj = hashB.getOrDefault(i, 0.0);
                result += a_ik * b_kj;
            }
            if (result != 0.0) {
                outputValue.set(Double.toString(result));
                context.write(key, outputValue);
            }
        }
    }
}
