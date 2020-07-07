# Distributed-Local-Outlier-Detection-on-Hadoop

- collaborate with @Nai-Tan Chang 


- [Medium Blog Post](https://medium.com/@leixxxli/distributed-lof-density-sensitive-anomaly-detection-with-mapreduce-6a34c176b90)



## Project Overview 

In anomaly detection, the local outlier factor (LOF) is an algorithm for finding anomalous data points by measuring the local deviation of a given data point with respect to its neighbors. The algorithm is suitable for detecting the outliers even if the data space has many areas with different densities. LOF is a centralized algorithm, i.e., runs on a single machine. This project is aimed to make LOF to be distributed that can run on Hadoop. Also, optimizations are implemented to speed up the execution. 

