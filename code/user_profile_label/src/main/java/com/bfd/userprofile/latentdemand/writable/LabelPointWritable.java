package com.bfd.userprofile.latentdemand.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class LabelPointWritable implements WritableComparable<LabelPointWritable> {
	private IntWritable type;
	private IntWritable index;
	private IntWritable value;

	public LabelPointWritable() {
		set(new IntWritable(), new IntWritable(),new IntWritable());
	}

	public LabelPointWritable(IntWritable index, IntWritable value, IntWritable type) {
		set(index, value , type);
	}

	public void set(IntWritable index, IntWritable value , IntWritable type) {
		this.index = index;
		this.value = value;
		this.type = type;
	}
	
	public IntWritable getType() {
		return type;
	}

	public IntWritable getIndex() {
		return index;
	}

	public IntWritable getValue() {
		return value;
	}

	public void readFields(DataInput in) throws IOException {
		index.readFields(in);
		value.readFields(in);
		type.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		index.write(out);
		value.write(out);
		type.write(out);
	}

	public int compareTo(LabelPointWritable pairWritable) {
        return index.compareTo(pairWritable.index); 
	}

	@Override
	public String toString() {
		//return String.format("%s:%s", new IntWritable(index.get()).get(),new IntWritable(value.get()).get());
		return String.format("%s:%s", index.get(),value.get());
	}
	
	
}
