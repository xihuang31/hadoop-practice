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

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.lib.*;

public  class WordPairCount {

    public static class WordPair implements WritableComparable<WordPair>{
        public String word1;
        public String word2;

        public WordPair(){}

        public void set(String word1,String word2){
            int compare = word1.toLowerCase().compareTo(word2.toLowerCase());
            if (compare <= 0){
                this.word1 = word1;
                this.word2 = word2;
            }else {
                this.word2 = word1;
                this.word1 = word2;
            }

        }

        @Override
        public void write(DataOutput out) throws IOException{
            out.writeUTF(word1);
            out.writeUTF(word2);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            word1 = in.readUTF();
            word2 = in.readUTF();
        }
        @Override
        public int compareTo(WordPair w){
            int compare1 = word1.toLowerCase().compareTo(w.word1.toLowerCase());
            int compare2 = word2.toLowerCase().compareTo(w.word2.toLowerCase());
            if(compare1 != 0){
                return compare1;
            }else{
                return compare2;
            }
        }
        @Override
        public String toString(){
          return "(" + this.word1 + "," + this.word2 + ")";
        }
    }


  public static class TokenizerMapper
       extends Mapper<Object, Text, WordPair, IntWritable>{

          private  WordPair wp  = new WordPair();
          private static String endword = new String("");
          private static String word1 = new String();
          private static String word2 = new String();
          private static final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
       String content = value.toString();
       String [] strs = content.split(" +");
       if (!endword.equals("")){
           word1 = normolizeWord(strs[0]);
           if (!word1.equals("")){
             wp.set(endword,word1);
             context.write(wp,one);
           }
       }
       for (int i=0;i<strs.length-1;i++){
         word1 = normolizeWord(strs[i]);
         word2 = normolizeWord(strs[i+1]);
         if (word1.equals("")){
           continue;
         }
         wp.set(word1,word2);
         context.write(wp,one);
       }
       endword = normolizeWord(strs[strs.length-1]);

        // StringTokenizer itr = new StringTokenizer(value.toString());
        // if (endword != null && itr.hasMoreTokens()){
        //     word1 = normolizeWord(itr.nextToken());
        //     wp.set(endword,word1);
        //     context.write(wp,one);
        // }
        // while (itr.hasMoreTokens()){
        //     word2 =
        // }

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
       extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(WordPair key, Iterable<IntWritable> values,
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
          extends Mapper<WordPair,IntWritable,IntWritable,WordPair>{

    public void map(WordPair key, IntWritable value, Context context)
        throws IOException, InterruptedException{
      context.write(value, key);
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
    Job countPair = Job.getInstance(conf, "word Pair count");
    countPair.setJarByClass(WordPairCount.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
          FileInputFormat.addInputPath(countPair, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(countPair,tempDir);

    countPair.setMapperClass(TokenizerMapper.class);
    countPair.setCombinerClass(IntSumReducer.class);
    countPair.setReducerClass(IntSumReducer.class);
    countPair.setOutputFormatClass(SequenceFileOutputFormat.class);
    countPair.setOutputKeyClass(WordPair.class);
    countPair.setOutputValueClass(IntWritable.class);

    Boolean wasSuccess = countPair.waitForCompletion(true) ;
    if (!wasSuccess) { return ; }
    System.out.println("WasSuccess"+wasSuccess);


    Job sortJob = Job.getInstance(new Configuration(), "Swap");
    sortJob.setJarByClass(WordPairCount.class);
    FileInputFormat.addInputPath(sortJob,tempDir);
    sortJob.setInputFormatClass(SequenceFileInputFormat.class);

    sortJob.setMapperClass(SwapMapper.class);
    sortJob.setOutputKeyClass(IntWritable.class);
    sortJob.setOutputValueClass(WordPair.class);
    sortJob.setSortComparatorClass(DescendingIntComparator.class);
    FileOutputFormat.setOutputPath(sortJob,new Path(otherArgs[otherArgs.length - 1]));
    System.exit(sortJob.waitForCompletion(true)?0:1);
  }



}
