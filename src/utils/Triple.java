package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/***
 * Triple Class, (subject,predicate,object)
 */
public class Triple implements WritableComparable<Triple>, Serializable {

	private static final long serialVersionUID = 7578125506159351415L;

	private String subject = "";
	private String predicate = "";
	private String object = "";
	private boolean isObjectLiteral = false;

	public Triple(String s, String p, String o, boolean literal) {
		this.subject = s;
		this.predicate = p;
		this.object = o;
		this.isObjectLiteral = literal;
	}
	
	public Triple() { }
	
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getSubject(){
		return this.subject;
	}
	
	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}
	public String getPredicate(){
		return this.predicate;
	}
	
	public void setObject(String object) {
		this.object = object;
	}
	public String getObject(){
		return this.object;
	}
	
	public boolean isObjectLiteral() {
		return isObjectLiteral;
	}

	public void setObjectLiteral(boolean isObjectLiteral) {
		this.isObjectLiteral = isObjectLiteral;
	}
	
	public String toString() {
		return subject + "\t" + predicate + "\t" + object;
	}


	public int hashCode() {
		//return HashFunctions.DJBHashInt(toString());
		long hash = 5381;
		String str = toString();
		for (int i = 0; i < str.length(); i++) {
			hash = ((hash << 5) + hash) + str.charAt(i);
		}

		return (int) (hash ^ (hash >>> 32));
	}

	@Override
	public int compareTo(Triple o) {
		if (subject == o.subject && predicate == o.predicate && object == o.object)
			return 0;
		else {
			if (subject.compareTo(o.subject) > 0)
				return 1;
			else
				return -1;
		}
			
	}
	
	public boolean equals(Object triple) {
		if (compareTo((Triple)triple) == 0)
			return true;
		else
			return false;
	}
	
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(Triple.class);
		}
		
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    	return compareBytes(b1, s1, l1 - 1, b2, s2, l2 - 1);
	    }
	}
	
	static {
		WritableComparator.define(Triple.class, new Comparator());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		subject = in.readUTF();
		predicate = in.readUTF();
		object = in.readUTF();
		isObjectLiteral = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(subject);
		out.writeUTF(predicate);
		out.writeUTF(object);
		out.writeBoolean(isObjectLiteral);
	}
	
	public static Triple read(DataInput in) throws IOException {
        Triple w = new Triple();
        w.readFields(in);
        return w;
    }
}
