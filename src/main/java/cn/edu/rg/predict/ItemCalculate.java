package cn.edu.rg.predict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ItemCalculate implements Writable
{

	private long user;
	private long basic;
	private long calculate;//要计算的项目
	private float basicRating;
	public void readFields(DataInput arg0) throws IOException
	{
		this.user=arg0.readLong();
		this.basic=arg0.readLong();
		this.calculate=arg0.readLong();
		this.basicRating=arg0.readFloat();
		
	}

	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeLong(this.user);
		arg0.writeLong(this.basic);
		arg0.writeLong(this.calculate);
		arg0.writeFloat(this.basicRating);
	}

	public long getUser()
	{
		return user;
	}

	public void setUser(long user)
	{
		this.user = user;
	}

	public long getBasic()
	{
		return basic;
	}

	public void setBasic(long basic)
	{
		this.basic = basic;
	}

	public long getCalculate()
	{
		return calculate;
	}

	public void setCalculate(long calculate)
	{
		this.calculate = calculate;
	}

	public float getBasicRating()
	{
		return basicRating;
	}

	public void setBasicRating(float basicRating)
	{
		this.basicRating = basicRating;
	}

}
