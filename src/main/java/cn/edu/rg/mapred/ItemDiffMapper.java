package cn.edu.rg.mapred;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.rg.KeyPair;
import cn.edu.rg.KeyPairValue;

public class ItemDiffMapper extends Mapper<KeyPair,KeyPairValue,KeyPair,KeyPairValue>
{

	@Override
	protected void map(KeyPair key, KeyPairValue value,Context context)
			throws IOException, InterruptedException
	{
		context.write(key, value);//不能直接输出，可以做优化
	}
	
}
