package cn.edu.rg.mapred.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import cn.edu.rg.ItemDiffInfo;
import cn.edu.rg.KeyPair;
import cn.edu.rg.predict.calculate.JoinTable;

public class TestReadJoinTable
{
	public static void main(String[] args) throws IOException
	{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		//FileStatus fst=fs.getFileStatus();
		//FSDataInputStream input= fs.open();
		Path path=new Path("/slopetest/output/joinTable/part-r-00000");
		SequenceFile.Reader file=new SequenceFile.Reader(fs,path,conf);
		LongWritable text=new LongWritable();
		JoinTable item=new JoinTable();
		System.out.println("user\tbase\ttarget\tbasicRating\tdiff\ttotalUser");
		while(file.next(text, item))
		{
			System.out.println(item.getUser()+"("+text.get()+")\t"+item.getBase()+"\t"+item.getToCalculate()+"\t"+item.getBasicRating()+"\t"+item.getDiff()+"\t"+item.getTotalUser());
		}
	}
}
