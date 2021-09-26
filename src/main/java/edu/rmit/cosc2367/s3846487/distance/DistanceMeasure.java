package edu.rmit.cosc2367.s3846487.distance;

import de.jungblut.math.DoubleVector;

public interface DistanceMeasure {

	public double measureDistance(double[] set1, double[] set2);

	public double measureDistance(DoubleVector vec1, DoubleVector vec2);

}
