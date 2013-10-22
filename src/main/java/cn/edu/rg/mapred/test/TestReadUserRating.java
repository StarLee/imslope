package cn.edu.rg.mapred.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import cn.edu.rg.Item;
import cn.edu.rg.User;

public class TestReadUserRating
{
	public static void main(String[] args) throws IOException
	{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		//FileStatus fst=fs.getFileStatus();
		//FSDataInputStream input= fs.open();
		Path path=new Path("/slopetest/rating/test");
		SequenceFile.Reader file=new SequenceFile.Reader(fs,path,conf);
		User text=new User();
		Item item=new Item();
		while(file.next(text, item))
		{
			System.out.println(text.getId()+":"+item.getId()+"/"+item.getRating()+"/");
		}
	}
}
