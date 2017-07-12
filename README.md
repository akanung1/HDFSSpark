# HDFSSpark
Dataset and problem statement: More info : http://sigspatial2016.sigspatial.org/giscup2016/problem

Solutions:

1. Read the input csv file from hadoop file system.
2. Then filter out the first line containing the column names.
3. Next each of the row is parsed and the required details are converted to a class Info containing
the latitude, longitude and day info.
4. A pair of RDD of tuples is created with key as Info string containing the latitude, longitude and
day info and value as 1.
5. After this, the RDD is reduced by key. Therefore we have a pairRDD with the number of points
in each cell. This pair RDD is also collected as a map in the driver program.
6. Next, mean is calculated using the all the values in the above pair RDD and dividing by total
number of cells i.e. 68200 (40*55*31). Using the mean the S value is calculated.
7. Now, for each cell in pairRDD represented by the key string, we find the valid neighboring cells
in the form of string and add them to a list. This string of lists is used to find the number of
points in the respective cell using the map saved above in the driver program. Hence, using the
given values, mean, S and total number of cells, we calculate the Getis Ord Statistic for this
cell. The neighboring cell weight is taken as 1 while calculating it.
8. Next we have a pair RDD containing the cell string and GetisOrd statistic, which is swapped,
sorted on the Getis Ord statistic and then reswapped. From this result we take the top 50 points.
This list converted to the required csv file and saved back in HDFS.


How to compile
==============

Just run "mvn clean install" when you're in source folder.
ALl the dependencies and compilation targets are mentioned in pom.xml file

Please note: The class file name is geospatial1.operation1.HotspotDetection.

We have used following command to run our program. Please refer it for more information.

Application reqires arg0 as input path and arg1 as output path.

Example command to run jar file
===============================
spark-submit \
--class geospatial1.operation1.HotspotDetection \
--master spark://osboxes:6066 \
--deploy-mode cluster \
--conf "spark.executor.memory=80g" \
--conf "spark.driver.memory=40g" \
distributed_daredevils_phase3.jar hdfs://localhost:54310/yellow.csv \
hdfs://localhost:54310/output
