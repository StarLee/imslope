package cn.edu.rg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * 记录用户的项目与评分
 * @author starlee
 *
 */
public class Item implements Writable{
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public float getRating() {
		return rating;
	}
	public void setRating(float rating) {
		this.rating = rating;
	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.id);
		out.writeFloat(this.rating);
		
	}
	public void readFields(DataInput in) throws IOException {
		this.id=in.readLong();
		this.rating=in.readFloat();
	}
	private long id;
	private float rating;
}
