package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TaxiIdAndErrorRate implements Comparable<TaxiIdAndErrorRate> {

	private final Text taxiId;
	private final DoubleWritable errorRate;

	public TaxiIdAndErrorRate(Text taxiId, DoubleWritable errorRate) {
		this.taxiId = taxiId;
		this.errorRate = errorRate;
	}

	public Text getTaxiId() {
		return taxiId;
	}

	public DoubleWritable getErrorRate() {
		return errorRate;
	}

	/**
	 * Compares two sort data objects by their value.
	 * 
	 * @param other
	 * @return 0 if equal, negative if this < other, positive if this > other
	 */
	@Override
	public int compareTo(TaxiIdAndErrorRate other) {
		return Double.compare(errorRate.get(), other.errorRate.get());
	}

	public String toString() {
		return "(" + taxiId.toString() + " , " + errorRate.toString() + ")";
	}
}
