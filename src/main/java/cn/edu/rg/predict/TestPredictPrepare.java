package cn.edu.rg.predict;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import cn.edu.rg.mapred.Test;
/**
 * 第一步
 * 得出用户要评分项目的表，去除了用户已经评过分的项目，这是第一步
 * @author starlee
 *
 */
public class TestPredictPrepare
{
	public static Job execute() throws IOException
	{
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Test.class);
		job.setJobName("prepare_predict_process");
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(PredictMapper.class);
		job.setReducerClass(PredictReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ItemPredictStatus.class);
		job.setOutputKeyClass(LongWritable.class);
		//job.setOutputValueClass(ItemCalculate.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("/slopetest/test.data"),new Path("/slopetest/predict.data"));
		Path output=new Path("/slopetest/output/predictPrepare");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(output))
		{
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Test.setup();
		Job job=TestPredictPrepare.execute();
		job.submit();
		while(true)
		{
			if(job.waitForCompletion(true))
			{
				break;
			}
		}
	}
}
