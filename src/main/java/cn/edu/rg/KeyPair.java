package cn.edu.rg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 两个项目差值对（并且保证始终base要少于compare）
 * 如果是用户项目对的话，base则是用户，项目是compare
 * @author starlee
 */
public class KeyPair implements WritableComparable
{

	private LongWritable baseKey=new LongWritable();
	private LongWritable compareKey=new LongWritable();
	public LongWritable getBaseKey()
	{
		return baseKey;
	}

	public void setBaseKey(LongWritable baseKey)
	{
		this.baseKey = baseKey;
	}

	public LongWritable getCompareKey()
	{
		return compareKey;
	}

	public void setCompareKey(LongWritable compareKey)
	{
		this.compareKey = compareKey;
	}

	
	public void readFields(DataInput arg0) throws IOException
	{
		this.baseKey.readFields(arg0);
		this.compareKey.readFields(arg0);
	}

	
	public void write(DataOutput arg0) throws IOException
	{
		this.baseKey.write(arg0);
		this.compareKey.write(arg0);
	}

	public int compareTo(Object obj)
	{
		KeyPair key=(KeyPair)obj;
		int base=this.baseKey.compareTo(key.getBaseKey());
		int compare=this.compareKey.compareTo(key.getCompareKey());
		if(base==1)
		{
			return 1;
		}
		if(base==-1)
		{
			return -1;
		}
		if(base==0)
		{
			if(compare==1)
				return 1;
			if(compare==-1)
				return -1;
		}
		return 0;
	}

}
