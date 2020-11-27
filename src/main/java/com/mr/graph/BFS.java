package com.mr.graph;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Graph is represented as adjacency list.
 * • Key: Node ID
 * • Value: EDGES|DISTANCE_FROM_SOURCE|COLOR|
 * Edges are list of connected node ids
 * source vertex color is GRAY other are WHITE
 * Vertex color can be WHITE GRAY BLACK
 * Key Value
 * 1    2,5|0|GRAY
 * 2    1,3,4,5|Integer.MAX_VALUE|WHITE
 * 3    2,4|Integer.MAX_VALUE|WHITE
 * 4    2,3,5|Integer.MAX_VALUE|WHITE
 * 5    1,2,4|Integer.MAX_VALUE|WHITE
 * <p>
 * <p>
 * Map Function:
 * – For each gray node, the mappers emit a new gray node, with distance =
 * distance + 1. they also then emit the input gray node, but colored black.
 * (once a node has been exploded, we're done with it.) mappers also emit
 * all non-gray nodes, with no change.
 * <p>
 * Reduce Function:
 * – the reducers job is to take all this data and construct a new node using
 * • the non-null list of edges
 * • the minimum distance
 * • the darkest color
 * <p>
 * When should we terminate search?
 * – Case1:if all the vertices are visited i.e colored black.
 * – Case2:When mapper finds the destined node with color gray
 * (i.e when mapper visits destined node for first time).
 */
public class BFS {

    private static final String TAB = "\\t";
    private static final String PIPE = "|";
    private static final String PIPE_SEP = "[|]";
    private static final String COMMA = ",";

    private static final String NULL = "NULL";

    private static final String WHITE = "WHITE";
    private static final String GRAY = "GRAY";
    private static final String BLACK = "BLACK";


    private class BfsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] keyVal = value.toString().split(TAB);
            IntWritable opKey = new IntWritable();
            Text opVal = new Text();
            opKey.set(Integer.parseInt(keyVal[0]));
            if (keyVal[1].endsWith(GRAY)) {
                String[] terms = keyVal[1].split(PIPE_SEP);
                String edges = terms[0];
                int distance = Integer.parseInt(terms[1]) + 1;
                opVal.set(String.join(PIPE, terms[0], terms[1], BLACK));
                context.write(opKey, opVal);
                for (String edge : edges.split(COMMA)) {
                    opKey.set(Integer.parseInt(edge));
                    opVal.set(String.join(PIPE, NULL, Integer.toString(distance), GRAY));
                    context.write(opKey, opVal);
                    // if graph has cycle then we have to maintain a set of all BLACK nodes
                    // then only emmit a node with GRAY color if its not visited i.e. not in BLACK node set
                }
            } else {
                context.write(opKey, opVal);
            }
        }
    }

    private class BfsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text opVal = new Text();
            Set<String> edges = new HashSet<>();
            int distance = Integer.MAX_VALUE;
            String color = WHITE;
            for (Text val : values) {
                String[] terms = val.toString().split(PIPE_SEP);
                if (!terms[0].equals(NULL)) {
                    edges.addAll(Arrays.asList(terms[0].split(COMMA)));
                }
                distance = Math.min(distance, Integer.parseInt(terms[1]));
                if (!terms[2].equals(color)) {
                    color = terms[2].equals(GRAY) && color.equals(WHITE) ?
                            terms[2] : terms[2].equals(BLACK) ? terms[2] : color;
                }
            }
            opVal.set(String.join(PIPE, String.join(COMMA, edges), Integer.toString(distance), color));
            context.write(key, opVal);
        }
    }
}
