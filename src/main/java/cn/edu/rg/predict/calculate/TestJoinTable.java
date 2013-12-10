package cn.edu.rg.predict.calculate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import cn.edu.rg.KeyPair;
import cn.edu.rg.mapred.Test;

public class TestJoinTable
{
	public static Job beJoinTable(Path target,Path diff) throws IOException
	{
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Test.class);
		job.setJobName("join_and_predict");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(KeyPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(JoinTable.class);
		job.setMapperClass(JoinTableMap.class);
		job.setReducerClass(JoinTableReduce.class);
		Path path=new Path("/slopetest/output/joinTable");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(path))
			fs.delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
		
		FileInputFormat.setInputPaths(job, target,diff);
		return job;
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Path target=new Path("/slopetest/output/predictPrepare/part-r-00000");
		Path diff=new Path("/slopetest/output/diff/part-r-00000");
		Job job=TestJoinTable.beJoinTable(target,diff);
		job.submit();
		while(true)
		{
			if(job.waitForCompletion(true))
				break;
		}
	}
}
