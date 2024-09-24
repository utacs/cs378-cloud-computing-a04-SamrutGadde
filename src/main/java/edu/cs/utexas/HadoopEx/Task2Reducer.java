package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text taxiId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int errors = 0;
        int total = 0;
        for (Text value : values) {
            String[] errorAndTotal = value.toString().split(",");
            errors += Integer.parseInt(errorAndTotal[0]);
            total += Integer.parseInt(errorAndTotal[1]);
        }

        double percentErrors = ((double) errors / total) * 100;
        System.out.println("Percent Errors: " + percentErrors + " Errors: " + errors + " Total: " + total);

        context.write(taxiId, new DoubleWritable(percentErrors));
    }
}
