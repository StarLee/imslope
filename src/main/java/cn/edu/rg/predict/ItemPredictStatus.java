package cn.edu.rg.predict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ItemPredictStatus implements Writable
{
	private long item;
	private byte tag;
	private long user;
	private float rating;//对于评分如果是已经评则直接加入就行了
	public void readFields(DataInput arg0) throws IOException
	{
		this.item = arg0.readLong();
		this.user = arg0.readLong();
		this.tag = arg0.readByte();
		this.rating=arg0.readFloat();
	}

	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeLong(this.item);
		arg0.writeLong(this.user);
		arg0.writeByte(this.tag);
		arg0.writeFloat(this.rating);
	}

	public float getRating()
	{
		return rating;
	}

	public void setRating(float rating)
	{
		this.rating = rating;
	}

	public long getItem()
	{
		return item;
	}

	public long getUser()
	{
		return user;
	}

	public void setUser(long user)
	{
		this.user = user;
	}

	public void setItem(long item)
	{
		this.item = item;
	}

	public byte getTag()
	{
		return tag;
	}

	public void setTag(byte tag)
	{
		this.tag = tag;
	}

}
