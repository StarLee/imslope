package cn.edu.rg.mapred.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import cn.edu.rg.ItemDiffInfo;
import cn.edu.rg.KeyPair;
import cn.edu.rg.predict.ItemCalculate;

/**
 * 读取计算表
 * 
 * @author starlee
 * 
 */
public class TestReadCalcute
{
	public static void main(String[] args) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// FileStatus fst=fs.getFileStatus();
		// FSDataInputStream input= fs.open();
		Path path = new Path("/slopetest/outbak/predictPrepare/part-r-00000");
		SequenceFile.Reader file = new SequenceFile.Reader(fs, path, conf);
		System.out.println("dfdf"+file.getValueClassName());
		LongWritable text = new LongWritable();
		ItemCalculate item = new ItemCalculate();
		
		while (file.next(text, item))
		{
			System.out.println("user:"+text.get()+"basic item:"+item.getBasic()+"("+item.getBasicRating()+")"+"calculate item:"+item.getCalculate());
		}
	}
}
