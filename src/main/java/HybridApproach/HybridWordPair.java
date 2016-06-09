package HybridApproach;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class HybridWordPair implements WritableComparable<HybridWordPair> {
	Text firstElement;
	Text secondElement;

	public HybridWordPair() {
		firstElement = new Text();
		secondElement = new Text();
	}

	public HybridWordPair(Text e1, Text e2) {
		this.firstElement = e1;
		this.secondElement = e2;
	}

	public HybridWordPair(String s1, String s2) {
		this(new Text(s1), new Text(s2));
	}

	public Text getFirstElement() {
		return firstElement;
	}

	public Text getSecondElement() {
		return secondElement;
	}

	public static HybridWordPair read(DataInput in) throws IOException {
		HybridWordPair WordPair = new HybridWordPair();
		WordPair.readFields(in);
		return WordPair;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		HybridWordPair wordPair = (HybridWordPair) o;

		if (secondElement != null ? !secondElement
				.equals(wordPair.secondElement)
				: wordPair.secondElement != null)
			return false;
		if (firstElement != null ? !firstElement.equals(wordPair.firstElement)
				: wordPair.firstElement != null)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = firstElement != null ? firstElement.hashCode() : 0;
		result = 163 * result
				+ (secondElement != null ? secondElement.hashCode() : 0);
		return result;
	}

	public void setFirstElement(Text firstElement) {
		this.firstElement = firstElement;
	}

	public void setSecondElement(Text secondElement) {
		this.secondElement = secondElement;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
//		System.out.println("read");
		firstElement.readFields(in);
		secondElement.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
//		System.out.println("write");
		firstElement.write(out);
		secondElement.write(out);

	}

	@Override
	public int compareTo(HybridWordPair other) {
		
//		System.out.println("compare");
		int returnVal = this.firstElement.compareTo(other.getFirstElement());
		if (returnVal != 0) {
			return returnVal;
		}
		
		else {
			if(this.secondElement.toString().equals(other.getSecondElement().toString()))
				return 0;
			if (this.secondElement.toString().equals("*")) {
				return -1;
			} else if (other.getSecondElement().toString().equals("*")) {
				return 1;
			}
		}
		
		return this.secondElement.compareTo(other.getSecondElement());
	}

	@Override
	public String toString() {
		return "< " + firstElement + ", " + secondElement + ">";
	}

}
