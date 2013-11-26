package cn.edu.rg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * 记录项目与项目之间的差值，并且保存用户个人评分信息,这个还是处于用户信息统计阶段,与keypair形成键值对的形式
 * @author starlee
 *
 */
public class KeyPairValue implements Writable
{
	//private long user;
	//private float baseRating;
	//private float compareRating;
	private float diff;
	private int number=1;
	
	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	/*public long getUser()
	{
		return user;
	}

	public void setUser(long user)
	{
		this.user = user;
	}

	public float getBaseRating()
	{
		return baseRating;
	}

	public void setBaseRating(float baseRating)
	{
		this.baseRating = baseRating;
	}

	public float getCompareRating()
	{
		return compareRating;
	}

	public void setCompareRating(float compareRating)
	{
		this.compareRating = compareRating;
	}*/

	public float getDiff()
	{
		return diff;
	}

	public void setDiff(float diff)
	{
		this.diff = diff;
	}

	
	
	public void readFields(DataInput arg0) throws IOException
	{
		//this.user=arg0.readLong();
		//this.baseRating=arg0.readFloat();
		//this.compareRating=arg0.readFloat();
		this.diff=arg0.readFloat();
		this.number=arg0.readInt();
		
	}
	public void write(DataOutput arg0) throws IOException
	{
		//arg0.writeLong(this.user);
		//arg0.writeFloat(this.baseRating);
		//arg0.writeFloat(this.compareRating);
		arg0.writeFloat(this.diff);
		arg0.writeInt(this.number);
	}

}
