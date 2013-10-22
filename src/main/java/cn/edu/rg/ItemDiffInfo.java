package cn.edu.rg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * 将统计信息封装
 * @author starlee
 *
 */
public class ItemDiffInfo implements Writable
{
	public long getTotalUser()
	{
		return totalUser;
	}
	public void setTotalUser(long totalUser)
	{
		this.totalUser = totalUser;
	}
	public float getTotalRating()
	{
		return totalRating;
	}
	public void setTotalRating(float totalRating)
	{
		this.totalRating = totalRating;
	}
	public float getAverageRating()
	{
		return averageRating;
	}
	public void setAverageRating(float averageRating)
	{
		this.averageRating = averageRating;
	}
	private long totalUser;
	private float totalRating;
	private float averageRating;

	
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(totalUser);
		out.writeFloat(totalRating);
		out.writeFloat(averageRating);
		
	}
	
	public void readFields(DataInput in) throws IOException
	{
		this.totalUser=in.readLong();
		this.totalRating=in.readFloat();
		this.averageRating=in.readFloat();
	
	}
	
}
