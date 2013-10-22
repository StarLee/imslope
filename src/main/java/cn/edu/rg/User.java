package cn.edu.rg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class User implements WritableComparable
{

	public User() {
		// TODO Auto-generated constructor stub
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}

	private long id;
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
	}

	
	public void readFields(DataInput in) throws IOException {
		this.id=in.readLong();
	}

	
	public int compareTo(Object o) {
		User user=(User)o;
		long that=user.id;
		return this.id>that?1:this.id==that?0:-1;
	}

}
