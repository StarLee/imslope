package cn.edu.rg.calculate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import cn.edu.rg.KeyPair;
import cn.edu.rg.mapred.Test;
import cn.edu.rg.predict.calculate.JoinTable;

public class TestCalculate
{
	public static Job execute(Path src_path) throws IOException
	{
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Test.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(CalculateMap.class);
		job.setMapOutputKeyClass(KeyPair.class);
		job.setMapOutputValueClass(JoinTable.class);
		job.setReducerClass(CalculateReduce.class);
		job.setOutputKeyClass(KeyPair.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, src_path);
		Path out=new Path("/slopetest/output/result");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(out))
			fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Path src_path=new Path("/slopetest/output/joinTable/part-r-00000");
		Job job=TestCalculate.execute(src_path);
		job.submit();
		while(true)
		{
			if(job.waitForCompletion(true))
				break;
		}
	}
}
