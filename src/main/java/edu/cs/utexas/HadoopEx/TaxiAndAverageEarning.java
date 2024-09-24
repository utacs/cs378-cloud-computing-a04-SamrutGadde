
    package edu.cs.utexas.HadoopEx;

    import org.apache.hadoop.io.DoubleWritable;
    import org.apache.hadoop.io.Text;
    
    public class TaxiAndAverageEarning implements Comparable<TaxiAndAverageEarning> {
    
        private final Text taxiId;
        private final DoubleWritable averageEarning;
    
        public TaxiAndAverageEarning(Text taxiId, DoubleWritable errorRate) {
            this.taxiId = taxiId;
            this.averageEarning = errorRate;
        }
    
        public Text getTaxiId() {
            return taxiId;
        }
    
        public DoubleWritable getAverageEarning() {
            return averageEarning;
        }
    
        /**
         * Compares two sort data objects by their value.
         * 
         * @param other
         * @return 0 if equal, negative if this < other, positive if this > other
         */
        @Override
        public int compareTo(TaxiAndAverageEarning other) {
            return Double.compare(averageEarning.get(), other.averageEarning.get());
        }
    
        public String toString() {
            return "(" + taxiId.toString() + " , " + averageEarning.toString() + ")";
        }
    }
       

