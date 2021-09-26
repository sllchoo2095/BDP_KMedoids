package edu.rmit.cosc2367.s3846487.Task3Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.rmit.cosc2367.s3846487.model.Medoid;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.mortbay.log.Log;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3846487.distance.EuclidianDistance;
import edu.rmit.cosc2367.s3846487.distance.DistanceMeasure;
import edu.rmit.cosc2367.s3846487.model.DataPoint;
import de.jungblut.math.DoubleVector;

/**
 * calculate a new centroid for these vertices
 */
public class KMedoidsReducer extends Reducer<Medoid, DataPoint, Medoid, DataPoint> {
	
	private static final Logger LOG = Logger.getLogger(KMedoidsReducer.class);
	/**
	 * A flag indicates if the clustering converges.
	 */
	public static enum Counter {
		CONVERGED
	}

	private final List<Medoid> centers = new ArrayList<>();
	
	private int iteration = 0;
	
	private DistanceMeasure distanceMeasurer;
	

	/**
	 * Having had all the dataPoints, we recompute the centroid and see if it converges by comparing previous centroid (key) with the new one.
	 *
	 * @param centroid 		key
	 * @param dataPoints	value: a list of dataPoints associated with the key (dataPoints in this cluster)
	 */
	@Override
	protected void reduce(Medoid medoid, Iterable<DataPoint> dataPoints, Context context) throws IOException,
			InterruptedException {
		Double totalDistance=0.0; 
		Medoid newMedoid = null;
		
		double nearestDistance = Double.MAX_VALUE;
		
		LOG.setLevel(Level.DEBUG);
		// This is for calculating the distance between a point and another (centroid is essentially a point).
		distanceMeasurer = new EuclidianDistance();

		List<DataPoint> pointsVectors = new ArrayList<>();
		
		
		//Compute Current centroid
		
		LOG.debug(" Mapper Input Current Medoid = "+ medoid + " cluster index = " + medoid.getClusterIndex() );
		
		Medoid currMedoid = new Medoid(medoid); 
		
		for (DataPoint dataValue : dataPoints) {
			LOG.debug(" dataValue added = "+ dataValue.toString() );
			pointsVectors.add(new DataPoint(dataValue)); 
			totalDistance += distanceMeasurer.measureDistance(currMedoid.getCenterVector(), dataValue.getVector()); 
		}

		//Calcualte the congfiguration cost 
		double currMedoidCost = totalDistance/ pointsVectors.size(); 
		
		//Calculate the distance between all of the data points
		
		for(int i =0; i<pointsVectors.size(); i++) {
			
			double sumData = 0.0; 
			
			for(int j =0 ; j<pointsVectors.size(); j++  ) {
				
				
					LOG.debug( "index i = "+ i + " index j = "+j); 
					LOG.debug(" pointsVectors.get(i).getVector() == "+ pointsVectors.get(i).getVector() + " pointsVectors.get(j).getVector() == "+ pointsVectors.get(j).getVector());
					double dist = distanceMeasurer.measureDistance(pointsVectors.get(i).getVector(),pointsVectors.get(j).getVector());
					sumData += dist; 
					
					
				}
			
				double costNewDataPoint= sumData/pointsVectors.size(); 
				
				if(costNewDataPoint < currMedoidCost) {
					currMedoid= new Medoid(pointsVectors.get(i)); 
					currMedoidCost = costNewDataPoint; 
				
			}
				LOG.debug("FINALISED currMedoid ***> "+ currMedoid);
		}
		
		LOG.debug("FINALISED currMedoid ***> "+ currMedoid);
		
		
		centers.add(currMedoid);
		
		

		// write new key-value pairs to disk, which will be fed into next round mapReduce job.
		for (DataPoint vector :pointsVectors) {
			
			
			LOG.debug("OUTPUT VECOTOR &&&>>> "+ vector.toString());
			LOG.debug("OUTPUT MEDOID "+currMedoid.toString());
			context.write(currMedoid, vector);
		}

		// check if all centroids are converged.
		// If all of them are converged, the counter would be zero.
		// If one or more of them are not, the counter would be greater than zero.
		if (currMedoid.update(medoid))
			context.getCounter(Counter.CONVERGED).increment(1);

	}

	/**
	 * Write the recomputed centroids to disk.
	 */
	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
				Medoid.class, IntWritable.class)) {

			//todo: serialize updated centroids.
			final IntWritable value = new IntWritable(iteration);
			System.out.println("Iteration Print: " + iteration );
			for (Medoid center : centers) {
				try {
					out.append(center, value);
					System.out.println("CenterPrint: "+center.toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
