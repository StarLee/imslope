package cn.edu.rg.mapred.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import cn.edu.rg.ItemDiffInfo;
import cn.edu.rg.KeyPair;

public class TestReadSequence
{
	public static void main(String[] args) throws IOException
	{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		//FileStatus fst=fs.getFileStatus();
		//FSDataInputStream input= fs.open();
		Path path=new Path("/slopetest/output/diff/part-r-00000");
		SequenceFile.Reader file=new SequenceFile.Reader(fs,path,conf);
		KeyPair text=new KeyPair();
		ItemDiffInfo item=new ItemDiffInfo();
		while(file.next(text, item))
		{
			//System.out.println(text.getBaseKey()+":"+text.getCompareKey()+"/"+item.getUser()+"/"+item.getBasicRating()+"/"+item.getCompairRating()+":(alluser)"+item.getTotalUser()+":(allrating:)"+item.getTotalRating()+":(a)"+item.getAverageRating());
			System.out.println(text.getBaseKey()+":"+text.getCompareKey()+"/"+":(alluser)"+item.getTotalUser()+":(allrating:)"+item.getTotalRating()+":(a)"+item.getAverageRating());
		}
	}
}
