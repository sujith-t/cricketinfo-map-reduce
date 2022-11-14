package com.cricketinfo.driver;

import com.cricketinfo.mapper.BallWicketRunsMapper;
import com.cricketinfo.mapper.TotalWicketsPerTeamMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class TeamWicketsCount {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: WicketRunsCount <in> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "Team Wicket Count");
        job.setJarByClass(TeamWicketsCount.class);

        job.setMapperClass(TotalWicketsPerTeamMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(TotalWicketsPerTeamMapper.WICKET_COUNTING_GROUP)) {
                System.out.println(counter.getDisplayName() + "," + counter.getValue());
            }
        }
    }
}
