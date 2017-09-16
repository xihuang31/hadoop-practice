/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Random;
import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String token = normolizeWord(itr.nextToken());
        word.set(token);
        context.write(word, one);
      }
    }

    public String normolizeWord(String word){
      if(word.endsWith("'")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith(")")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith("_")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith(";")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith("!")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith("?")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith(",")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith(":")){
          word = word.substring(0,word.length()-1);
        }
        if(word.endsWith(";")){
            word = word.substring(0,word.length()-1);
        }
        if(word.endsWith("--")){
          word = word.substring(0,word.length()-2);
        }
        if(word.endsWith("'s")){
          word = word.substring(0,word.length()-2);
        }
        if(word.endsWith("ly")){
          word = word.substring(0,word.length()-2);
        }
        if(word.endsWith("ed")){
          word = word.substring(0,word.length()-2);
        }
        if(word.endsWith("ing")){
          word = word.substring(0,word.length()-3);
        }
        if(word.endsWith("ness")){
          word = word.substring(0,word.length()-4);
        }
        if(word.startsWith("'")){
          word = word.substring(0,word.length()-1);
        }
        
        if(word.startsWith("(")){
          word = word.substring(0,word.length()-1);
        }
        if(word.startsWith("_")){
          word = word.substring(0,word.length()-1);
        }
        if(word.startsWith("\"")){
          word = word.substring(0,word.length()-1);
        }
        word = word.toLowerCase();
        return word;
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

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

    public static class SwapMapper
        extends Mapper<LongWritable, IntWritable, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, IntWritable value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            System.out.println("+++++"+value.toString());
            word.set(itr.nextToken());
            one.set(Integer.parseInt(itr.nextToken()));
            context.write(one,word);
        }
    }

    public static class DescendingIntComparator extends WritableComparator {

        public DescendingIntComparator() {
            super(IntWritable.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
      Path tempDir =
      new Path("grep-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
      for (int i = 0; i < otherArgs.length - 1; ++i) {
          FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
      }
    FileOutputFormat.setOutputPath(job,tempDir);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    Boolean wasSuccess = job.waitForCompletion(true) ;
    if (!wasSuccess) { return ; }
    System.out.println("WasSuccess"+wasSuccess);


    Job job2 = Job.getInstance(new Configuration(), "Swap");
    job2.setJarByClass(WordCount.class);
    FileInputFormat.addInputPath(job2,tempDir);
    job2.setInputFormatClass(SequenceFileInputFormat.class);

    job2.setMapperClass(InverseMapper.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setNumReduceTasks(1);
    job2.setSortComparatorClass(DescendingIntComparator.class);
    FileOutputFormat.setOutputPath(job2,new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job2.waitForCompletion(true)?0:1);
  }



}
