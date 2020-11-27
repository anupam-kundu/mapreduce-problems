package com.mr.sample;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Given a paragraph , set of words (rows and columns)
 * create inverted index for each word
 * result : if we search any word in the inverted index then it should be able to return
 * locations of the word (row and column)
 * <p>
 * example : input
 * Google News is a news aggregator service developed by Google. It presents
 * a continuous flow of articles organized from thousands of publishers and
 * magazines. Google News is available as an app on Android, iOS, and the
 * Web. Google released a beta version in September 2002 and the official
 * app in January 2006.
 * <p>
 * output:
 * is -> (splitName,0,2) , (splitName,2,3)
 * a -> (splitName,0,3), (splitName,1,0), (splitName,3,3)
 */
public class InvertedIndex {
    private static final String COMMA = ",";
    private static final String SPACE = " ";

    private class InvertedIdxMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String splitName;
        private long lineNumber;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            lineNumber = 0;
            splitName = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(SPACE);
            Text opKey = new Text();
            Text opVal = new Text();
            for (int col = 0; col < words.length; col++) {
                opKey.set(words[col]);
                opVal.set("(" + String.join(COMMA, splitName, Long.toString(lineNumber)
                        , Integer.toString(col)) + ")");
                context.write(opKey, opVal);
                lineNumber++;
            }
        }
    }

    private class InvertedIdxReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> indexValues = new ArrayList<>();
            values.forEach(value -> indexValues.add(value.toString())); // will not work for huge index size
            context.write(key, new Text(String.join(COMMA, indexValues)));
        }
    }

}
