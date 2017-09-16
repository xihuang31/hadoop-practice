
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
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
import org.apache.hadoop.util.*;



public class TaxReturnSum {

  public static class StateLevel implements WritableComparable<StateLevel>{
    public String state;
    public int level;

    public StateLevel(){}

    public void set(String state, int level){
      this.state = state;
      this.level = level;
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(state);
      out.writeInt(level);
    }
    @Override
    public void readFields(DataInput in) throws IOException{
      state = in.readUTF();
      level = in.readInt();
    }
    @Override
    public int compareTo(StateLevel s){
      int compare = state.toUpperCase().compareTo(s.state.toUpperCase());
      if (compare != 0){
        return compare;
      }else{
        return this.level - s.level;
      }
    }

    @Override
    public String toString(){
      return "(" + state + "," + level + ")" ;
    }
  }

  public static class TokenMapper extends
          Mapper<Object,Text,StateLevel,DoubleWritable> {
      private StateLevel stateLevel = new StateLevel();
      private DoubleWritable tax = new DoubleWritable(0);

      public void map(Object key, Text value, Context context)
        throws IOException,InterruptedException{
          String content = value.toString();
          String [] strs = content.split(",");

          if(strs[1].equals("STATE")){
            return ;
          }
          stateLevel.set(strs[1],Integer.parseInt(strs[3]));
          tax.set(Double.parseDouble(strs[4]));
          context.write(stateLevel,tax);
      }

  }

  public static class DoubleSumReducer extends
    Reducer<StateLevel,DoubleWritable,StateLevel,DoubleWritable>{
    private DoubleWritable result = new DoubleWritable();

    public void reduce(StateLevel key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException{
      double sum = 0.0;
      for (DoubleWritable val:values){
        sum += val.get();
      }
      result.set(sum);
      context.write(key,result);
    }
  }


  public static void main(String[] args) throws Exception{

      Configuration conf = new  Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 2) {
          System.err.println("Usage: taxcount <in> [<in>...] <out>");
          System.exit(2);
      }
      Job job =  Job.getInstance(conf,"Tax Return Sum");
      job.setJarByClass(TaxReturnSum.class);
      FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
      job.setMapperClass(TokenMapper.class);
      job.setReducerClass(DoubleSumReducer.class);
      job.setOutputKeyClass(StateLevel.class);
      job.setOutputValueClass(DoubleWritable.class);
      System.exit( job.waitForCompletion(true)?0:1);
  }


}
