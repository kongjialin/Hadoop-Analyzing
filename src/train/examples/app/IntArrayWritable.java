package train.examples.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;

public class IntArrayWritable extends ArrayWritable {
	
	public IntArrayWritable() {
		super(IntWritable.class);
	}

	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < get().length; i++) {
			sb.append(((IntWritable)get()[i]).toString());
			if(i != get().length - 1)
				sb.append(",");
		}
		
		return sb.toString();
	}
	
}
