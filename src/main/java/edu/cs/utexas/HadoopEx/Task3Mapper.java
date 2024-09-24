package edu.cs.utexas.HadoopEx;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
		String taxiId = line[0];
        double totalAmount = 0.0; 
        int tripTime = 0; 
		try {
			totalAmount = Double.parseDouble(line[16]);
            tripTime = Integer.parseInt(line[4]); 
            if (tripTime == 0) {
                return;
            }
		} catch (NumberFormatException e) {
			System.out.println(e.getMessage());
		}
        Text amount = new Text( totalAmount + "," + tripTime);
		context.write(new Text(taxiId), amount);

	}
}

