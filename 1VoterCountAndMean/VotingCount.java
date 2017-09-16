import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VotingCount {


  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable count = new IntWritable();
    private Text district = new Text();
    private final static IntWritable people = new IntWritable(0);
    private final static LongWritable longOne = new LongWritable(1);

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        //StringTokenizer itr = new StringTokenizer(value.toString());
        //LongWritable k = (LongWritable) key;
        //district.set(itr.nextToken());
//        if (longOne.equals(key) ){
//            System.out.println("---------------");
//        }
//        System.out.println(key);
        String content = value.toString();
        String [] strs = content.split(",");
        if (strs[0].equals("Code for district")){
            return;
        }
        district.set(strs[0]);
        people.set(Integer.parseInt(strs[3]));

        context.write(district,people);

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(0);

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class myMapper
       extends Mapper<Object , Text , IntWritable, IntWritable>{

    private IntWritable one = new IntWritable(1);
    private IntWritable res = new IntWritable(0);
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        System.out.println(itr.nextToken());
        //itr.nextToken();
        res.set(Integer.parseInt(itr.nextToken()));
        context.write(one,res);
    }
  }

  public static class meanReducer
       extends Reducer<IntWritable,IntWritable,Text,DoubleWritable> {
    private IntWritable result = new IntWritable(0);
    private Text res = new Text("Mean");
    private DoubleWritable mean = new DoubleWritable(0);
    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0.0;
      double count = 0.0;
      for (IntWritable val : values) {
        sum += val.get();
        count += key.get();
      }
      mean.set((Double)sum/count);
      context.write(res, mean);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "voting count");
    job.setJarByClass(VotingCount.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/a"));
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "mean");
    job2.setJarByClass(VotingCount.class);
    job2.setMapperClass(myMapper.class);
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(IntWritable.class);
//    job2.setCombinerClass(meanReducer.class);
    job2.setReducerClass(meanReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    job2.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job2, new Path(args[1]+"/a"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/b"));
    System.exit(job2.waitForCompletion(true)?1:0);
  }
}
