package com.cricketinfo.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalWicketsPerTeamMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final String WICKET_COUNTING_GROUP = "wicketCounter";
    private static final int INX_IS_WICKET = 11;
    private static final int INX_BOWLING_TEAM = 17;

    @Override
    public void map(Object key, Text value, Context context) {
        String[] tokens = value.toString().split(",");

        String team = tokens[INX_BOWLING_TEAM];
        int isWicket = Integer.parseInt(tokens[INX_IS_WICKET]);

        //wicket taken
        if(isWicket == 1) {
            context.getCounter(WICKET_COUNTING_GROUP, team).increment(1);
        }
    }
}
