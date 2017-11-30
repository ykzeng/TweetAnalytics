import java.awt.JobAttributes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class TweetTimePartitioner{
  public static class PeriodMapper extends Mapper<LongWritable, Text, IntWritable,
         LongWritable>{
	static enum CountersEnum { TWEET_COUNTS };
	  
    private final static LongWritable one = new LongWritable(1);
    private Configuration conf;
    static Logger logger = Logger.getLogger(
            TweetTimePartitioner.class.getName());

//    @Override
//    public void setup(Context context) throws IOException,
//        InterruptedException {
//      // we skip this part since we have nothing to setup, no params to parse
//      //conf = context.getConfiguration();
//    }

    @Override
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // we should be using this maybe when analyzing sleep
      //String line = (caseSensitive) ?
      //    value.toString() : value.toString().toLowerCase();
      String line = value.toString(), prefix = "", time = "", date = "";
      StringTokenizer itr = new StringTokenizer(line);
      // acquire the first token in this line
      if (itr.hasMoreTokens()) {
          prefix = itr.nextToken();
      }
      else
    	  return;
      if (prefix.equals("T")) {
          try {
        	  date = itr.nextToken();
			  time = itr.nextToken();
//			  logger.log(Level.INFO, ("prefix: " + prefix
//						+ "\ndate: " + tmp
//						+ "\ntime: " + time));
	          int hour = Integer.valueOf(time.split(":")[0]);
	          context.write(new IntWritable(hour), one);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.log(Level.SEVERE, ("prefix: " + prefix
						+ "\ndate: " + date
						+ "\ntime: " + time));
			}
      }


      Counter counter = context.getCounter(CountersEnum.class.getName(),
          CountersEnum.TWEET_COUNTS.toString());
      counter.increment(1);
    }
  }
  
  public static class PeriodSumReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable>{
	  
	  private LongWritable result = new LongWritable();
	  
	  public void reduce(IntWritable key, Iterable<LongWritable> values,
              Context context
              ) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	  }
  }
  
  public static void main(String[] args) throws Exception{
	  Configuration conf = new Configuration();
	  GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
	  String[] remainingArgs = optionParser.getRemainingArgs();
	  if (remainingArgs.length != 2) {
	      System.err.println("Usage: tweetstat <in> <out>");
	      System.exit(2);
	  }
	  
	  Job job = Job.getInstance(conf, "tweet time partitioner");
	  job.setJarByClass(TweetTimePartitioner.class);
	  job.setMapperClass(PeriodMapper.class);
	  job.setCombinerClass(PeriodSumReducer.class);
	  job.setReducerClass(PeriodSumReducer.class);
	  job.setOutputKeyClass(IntWritable.class);
	  job.setOutputValueClass(LongWritable.class);
	  
	  FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
	  FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
	    
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
