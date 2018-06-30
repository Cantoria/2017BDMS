/**
* @author Yinghan Shen
* @version 1.0
* Modified by Yinghan Shen for BDMS Hw2, Part1
*/

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



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.ArrayWritable;

public class Hw2Part1 {

  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
	private Text word = new Text();
	/*
		make a filter for each line
		@param line data line to filt
		@return true means data format is right, otherwise is wrong
	*/
    public static Boolean filtLine(String line) throws NumberFormatException, NullPointerException{
		try{
			String[] s = line.split("\\s+");
			if(s.length!=3 || s[0].getClass().toString().equals("String")||s[1].getClass().toString().equals("String")||Double.valueOf(s[2]).getClass().toString().equals("Double")){
				return false;
			}
			else{
				return true;
			}
		}catch(NumberFormatException nfe){
			return false;
		}catch(NullPointerException npe){
			return false;
		}		
	}
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	  
	  //filt wrong format data
	  if(TokenizerMapper.filtLine(value.toString()) != false){
		  String[] parts = value.toString().split("\\s+");
		  String k = parts[0] + " " + parts[1];
		  word.set(k);
		  DoubleWritable time = new DoubleWritable(Double.parseDouble(parts[2]));
		  context.write(word, time);
		}  
	  }

  }
  


  // This is the Reducer class
  // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
  //

  public static class CountAvgReducer
       extends Reducer<Text,DoubleWritable,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();


    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
	  int count = 0;
	  //calculate avg time and count frequency
      for (DoubleWritable val : values) {
        sum += val.get();
		count++;
      }
	  double avg = sum / count;
	  String value = Integer.toString(count) + " " + String.format("%.3f", avg);
      // generate result key
      result_key.set(key);

      // generate result value
      result_value.set(value);

      context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	conf.set("mapred.textoutputformat.separator"," ");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: Hw2Part1 <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Count&Avg");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(CountAvgReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
