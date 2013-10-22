package cn.edu.rg.mapred.test;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.rg.mapred.Test;

public class TextUser
{
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, IOException
	{
		Test.setup();
		Job job=Test.statisticsUser();
		job.submit();
		while(true)
		{
			if(job.waitForCompletion(true))
				break;
		}
		System.out.println("execute success-------------------------------------");
		System.out.println(FileOutputFormat.getOutputPath(job).getName());
		System.out.println("execute success-------------------------------------");
	}
}
