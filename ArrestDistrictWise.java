package org.myorg;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.apache.hadoop.util.GenericOptionsParser;
public class ArrestDistrictWise {
public static void main(String [] args) throws Exception
{
	//driver class for declaring the mapper/reducer/configurations
Configuration c=new Configuration();
String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
Path input=new Path(files[0]);
Path output=new Path(files[1]);
Job j=new Job(c,"CrimeAnalysis");
j.setJarByClass(ArrestDistrictWise.class);
j.setCombinerClass(ReduceForCrime.class);
j.setMapperClass(MapForCrime.class);
j.setReducerClass(ReduceForCrime.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(j, input);
FileOutputFormat.setOutputPath(j, output);
System.exit(j.waitForCompletion(true)?0:1);
}
//mapper class
public static class MapForCrime extends Mapper<LongWritable, Text, Text, IntWritable>{
public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
{
String line = value.toString();
String lineO = line.replaceAll("\""," ");
String[] words=lineO.split(",");
try 
{
  Text StrDistTxt = new Text(words[11].toUpperCase().trim());
  IntWritable CaseCount = new IntWritable(1);
  String StrDistrict = words[11].toUpperCase().trim();         // element 11 is gives the district value
  String StrArrests = words[8].toUpperCase().trim();     // element 8 is gives the arrest value -- true indicates arrested
   
	  if(StrArrests.equals("TRUE"))                         // check if arrest is true, add the district in the context            
	  {
  con.write(StrDistTxt, CaseCount); //write to context which will fed as input to the reducer
	  }
  
}
catch(Exception e)
{
	System.out.println("line to be missed"+lineO);
}
}
}
//Reducer  class
public static class ReduceForCrime extends Reducer<Text, IntWritable, Text, IntWritable>
{
public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
{
int sum = 0;
   for(IntWritable value : values)
   {
   sum += value.get();
   }
   con.write(word, new IntWritable(sum));
}
}
}