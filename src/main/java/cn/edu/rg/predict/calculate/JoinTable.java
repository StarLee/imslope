package cn.edu.rg.predict.calculate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 得到差值信息，参考标准项目，以及当前用户对其的评分，根据这个表中的信息就可以计算用户对项目I的预测评分了
 * @author starlee
 *
 */
public class JoinTable implements Writable
{
	private long base;//参才标准项
	private long toCalculate;//要计算的项目
	private long user;//用户
	private float basicRating;//用户对参考标准项的评分
	private float diff;//要计算项与参考项之间的差值
	//private long totalValue;//总评分值不要了，没有什么意义
	private long totalUser;//这种差值的有几个用户
	
	public JoinTable()
	{
		
	}
	public JoinTable(long base,long toCalculate,long user,float basicRating,float diff,long totalUsers)
	{
		this.base=base;
		this.toCalculate=toCalculate;
		this.user=user;
		this.basicRating=basicRating;
		this.diff=diff;
		this.totalUser=totalUsers;
	}
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.base);
		out.writeLong(this.toCalculate);
		out.writeLong(this.user);
		out.writeFloat(this.basicRating);
		out.writeFloat(this.diff);
		out.writeLong(this.totalUser);
	}

	public void readFields(DataInput in) throws IOException
	{
		this.base=in.readLong();
		this.toCalculate=in.readLong();
		this.user=in.readLong();
		this.basicRating=in.readFloat();
		this.diff=in.readFloat();
		this.totalUser=in.readLong();
		
	}
	public long getToCalculate()
	{
		return toCalculate;
	}
	public void setToCalculate(long toCalculate)
	{
		this.toCalculate = toCalculate;
	}
	public long getBase()
	{
		return base;
	}
	public void setBase(long base)
	{
		this.base = base;
	}
	
	public long getUser()
	{
		return user;
	}
	public void setUser(long user)
	{
		this.user = user;
	}
	public float getBasicRating()
	{
		return basicRating;
	}
	public void setBasicRating(float basicRating)
	{
		this.basicRating = basicRating;
	}
	public float getDiff()
	{
		return diff;
	}
	public void setDiff(float diff)
	{
		this.diff = diff;
	}
	public long getTotalUser()
	{
		return totalUser;
	}
	public void setTotalUser(long totalUser)
	{
		this.totalUser = totalUser;
	}
}
