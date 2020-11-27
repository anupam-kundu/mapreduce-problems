package com.mr.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * https://stanford.edu/~rezab/classes/cme323/S16/projects_reports/bodoia.pdf
 */
public class KMeansClustering {

    // TBD

    private class kMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    }

    private class kMeansReduce extends Reducer<Text, Text, Text, Text> {

    }
}
