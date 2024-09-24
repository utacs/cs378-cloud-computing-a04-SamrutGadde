package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text taxiId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int triptime = 0; 
        Double total = 0.0;
        for (Text value : values) {
            String[] TaxiAndAverageEarning = value.toString().split(",");
            total += Double.parseDouble(TaxiAndAverageEarning[0]);
            triptime += Integer.parseInt(TaxiAndAverageEarning[1]);
        }

        double averageEarning = ((double) total / (triptime / 60.0)); // converted it to a double here
        System.out.println("Average Earning: " + averageEarning);

        context.write(taxiId, new DoubleWritable(averageEarning));
    }
}
