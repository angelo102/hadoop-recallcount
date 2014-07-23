import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

//import org.apache.hadoop.util.*;

public class recallcount {

	//Mapper for Counting only by year
	public static class MapYear extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text fieldKey = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] line = value.toString().split("\t"); //split line by tabs

			//Get Year to set as key
			if(line[15].length()>0){
				String year = line[15].substring(0, 4);
				fieldKey.set(year);
			}
			else{
				fieldKey.set("NO_YEAR");
			}

			output.collect(fieldKey, one);
		}
	}

	//Mapper for Counting by year and make
	public static class MapYearAndMake extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text fieldKey = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] line = value.toString().split("\t"); //split line by tabs

			//Get Year and make to set as key

			//Get Year to set as key
			String year="";
			if(line[15].length()>0)
				year = line[15].substring(0, 4);
			else
				year ="NO_YEAR";

			//get make
			String make = line[2];

			fieldKey.set(year+"-"+make);

			output.collect(fieldKey, one);
		}
	}

	//Mapper for Counting by year, make and cause
	public static class MapYearModelAndCause extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text fieldKey = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] line = value.toString().split("\t"); //split line by tabs

			//Get Year, make and cause to set as key

			//Get Year to set as key
			String year="";
			if(line[15].length()>0)
				year = line[15].substring(0, 4);
			else
				year ="NO_YEAR";

			//Get Model
			String model = line[3];

			//Get the principal cause of the recall by removing sub causes
			String[] causeArray = line[6].split(":");
			String[] causeArray2 = causeArray[0].split(",");
			String cause = causeArray2[0];

			fieldKey.set(year+"-"+model+"-"+cause);

			output.collect(fieldKey, one);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Path inputDataPath = new Path(args[0]);
		Path outputDataPath = new Path(args[1]);
		String jobTitle = args[2];
		int numMappers = Integer.parseInt(args[3]);
		int numReducers = Integer.parseInt(args[4]);
		int jobType = Integer.parseInt(args[5]); //1: year, 2: year and make, 3: year, make and cause

		JobConf conf = new JobConf(recallcount.class);

		//JobName from arguments
		conf.setJobName(jobTitle);

		//Set mappers and reducers
		
		//Split size to be able to set mappers
		String mbSplitSize = "134217728";
		switch(numMappers){
			case 1: mbSplitSize="134217728";
					break;
			case 2: mbSplitSize="67108864";
					break;
			case 10: mbSplitSize="10485760";
					break;
			default: mbSplitSize="134217728";
					break;
		}
		conf.set("mapred.min.split.size", mbSplitSize);
		conf.set( "dfs.block.size", mbSplitSize); 

		conf.setNumMapTasks(numMappers); //set num of mappers (only suggests)
		conf.setNumReduceTasks(numReducers); //set number of reducers

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		//set the mapper class based on selected arg
		switch(jobType){
			case 1: conf.setMapperClass(MapYear.class);
					break;
			case 2: conf.setMapperClass(MapYearAndMake.class);
					break;
			case 3: conf.setMapperClass(MapYearModelAndCause.class);
					break;
			default: conf.setMapperClass(MapYear.class);
					break;	
		}

		//set combiner and reducer class
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		//set input and output format classes
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//set inputpath and outputpath
		FileInputFormat.setInputPaths(conf, inputDataPath);
		FileOutputFormat.setOutputPath(conf, outputDataPath);

		//Get Initial Time
		long init = System.nanoTime();

		//Run the Job
		JobClient.runJob(conf);

		//Get Final Time
		long fin = System.nanoTime();

		//Time it took
		long elapsedTime = fin - init;		
		long elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);

		//Write Results to a file
		try {
			FileWriter fw;
			fw = new FileWriter("benchmarks.txt",true);
			fw.append("Job Title: " + jobTitle + "\n");
			fw.append("Number Mappers: " + String.valueOf(numMappers) + "\n");
			fw.append("Number Reducers: " + String.valueOf(numReducers) + "\n");
			fw.append("Split Size in Bytes: " + mbSplitSize + "\n");
			fw.append("Initial Time in Nano Seconds: " + String.valueOf(init) + "\n");
			fw.append("Final Time in Nano Seconds: " + String.valueOf(fin) + "\n");
			fw.append("Time in Nano Seconds: " + String.valueOf(elapsedTime) + "\n");
			fw.append("Time in Seconds: " + String.valueOf(elapsedSeconds) + "\n");
			fw.append("***End Benchmark***\n\n\n");
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}