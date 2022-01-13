# BDP_KMedoids

Task 3: Implement PAM Algorithm with a MapReduce Program 

The basic idea of the PAM algorithm is to reassign the medoid (centre of the cluster) within each cluster based on the shortest distance. 

This program takes input in the form of numeric coordinates. In this case like the file ``NYTaxiLC1000``

hadoop fs -copyToLocal /user/NAME/NAME_BDP_A2_Task3-0.0.1-SNAPSHOT.jar ~/

hadoop jar NAME_BDP_A2_Task3-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.NAME.NAME_BDP_A2_Task3.KMedoidsClusteringJob <input path with NYTaxiLC1000> <output path> <k value int 1-6>

The Java code outputs stringing coordinates. 

The python code was used to clean the output of the 
