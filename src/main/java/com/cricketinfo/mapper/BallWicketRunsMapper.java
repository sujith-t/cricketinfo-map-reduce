package com.cricketinfo.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;

public class BallWicketRunsMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final String BALL_COUNTING_GROUP = "ballCounter";
    private static final HashMap<Integer, String> INDEXES = new HashMap<>();

    @Override
    public void map(Object key, Text value, Context context) {
        String[] tokens = value.toString().split(",");

        for(Map.Entry<Integer,String> e : INDEXES.entrySet()) {
            int intVal = Integer.parseInt(tokens[e.getKey()]);

            //only effective for wickets taken and extra runs given OR
            //no runs scored
            if((intVal > 0 && !e.getValue().equals("NO_RUNS_SCORED")) || (intVal == 0 && e.getValue().equals("NO_RUNS_SCORED"))) {
                context.getCounter(BALL_COUNTING_GROUP, e.getValue()).increment(1);
            }
        }
    }

    @Override
    public void setup(Context context) {
        INDEXES.put(11, "WICKETS_TAKEN");
        INDEXES.put(8, "EXTRA_RUNS_GIVEN");
        INDEXES.put(9, "NO_RUNS_SCORED");
    }
}
