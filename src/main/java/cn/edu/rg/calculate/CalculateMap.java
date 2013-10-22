package cn.edu.rg.calculate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.rg.KeyPair;
import cn.edu.rg.predict.calculate.JoinTable;

public class CalculateMap extends Mapper<LongWritable,JoinTable,KeyPair,JoinTable>
{

	@Override
	protected void map(LongWritable key, JoinTable value,Context context)
			throws IOException, InterruptedException
	{
		KeyPair keypair=new KeyPair();
		keypair.setBaseKey(new LongWritable(value.getUser()));
		keypair.setCompareKey(new LongWritable(value.getToCalculate()));
		context.write(keypair, value);
	}
}
