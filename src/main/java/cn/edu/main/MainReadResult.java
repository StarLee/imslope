package cn.edu.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;

import cn.edu.rg.ItemDiffInfo;
import cn.edu.rg.KeyPair;
/**
 * 
 * @author starlee
 *
 */
public class MainReadResult
{
	public static void main(String[] args) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("/slopetest/output/result/part-r-00000");
		SequenceFile.Reader file = new SequenceFile.Reader(fs, path, conf);
		KeyPair text = new KeyPair();
		FloatWritable item = new FloatWritable();
		System.out.println("user\ttarget\tpredictRating");
		while (file.next(text, item))
		{
			System.out.println(text.getBaseKey() + "\t" + text.getCompareKey()+"\t"+item.get());
		}
		file.close();
	}
}
