# NYC Taxi Trip Analysis Using Hadoop MapReduce

## Project Overview

This project analyzes the New York City Taxi Trip Duration dataset using **Apache Hadoop MapReduce and Java**. The objective is to process a large dataset of taxi trips and identify useful transportation patterns, including peak pickup hours, average trip duration, vendor activity, passenger distribution, and daily trip trends.

The project was implemented using Google Colab, Hadoop HDFS, Java MapReduce, and Python for data visualization.

## Dataset Information

- **Dataset:** NYC Taxi Trip Duration
- **Source:** Kaggle
- **Year:** 2016
- **File:** `train.csv`
- **Number of records:** 1,458,644
- **File size:** Approximately 191.3 MB

Dataset link: https://www.kaggle.com/competitions/nyc-taxi-trip-duration/data

## Technologies Used

- Java
- Apache Hadoop 3.5.0
- Hadoop Distributed File System (HDFS)
- Hadoop MapReduce
- Google Colab
- Python
- Pandas
- Matplotlib

## Project Structure

```text
NYC-Taxi-Hadoop-MapReduce/
│
├── Charts/
│   ├── 1_trips_by_hour.png
│   ├── 2_average_duration.png
│   ├── 3_vendor_analysis.png
│   ├── 4_passenger_distribution.png
│   └── 5_daily_trips.png
│
├── Results/
│   ├── average_duration.txt
│   ├── daily_trips.txt
│   ├── evidence_hourly.txt
│   ├── hourly_trips.txt
│   ├── passenger_distribution.txt
│   └── vendor_analysis.txt
│
├── Java_Source_Code/
│   ├── AverageDurationByHour.java
│   ├── DailyTrips.java
│   ├── PassengerDistribution.java
│   ├── TaxiTripsByHour.java
│   └── TripsByVendor.java
│
├── JAR_Files/
│   ├── average-duration.jar
│   ├── daily-trips.jar
│   ├── passenger-analysis.jar
│   ├── taxi-mapreduce.jar
│   └── vendor-analysis.jar
│
├── Screenshots/
│   └── MapReduce execution evidence
│
└── README.md
```

## MapReduce Jobs Implemented

Five Java MapReduce jobs were developed and executed successfully:

| No. | MapReduce Job | Purpose |
|---|---|---|
| 1 | Trips by Hour | Counts taxi trips for each pickup hour |
| 2 | Average Duration by Hour | Calculates average trip duration for each pickup hour |
| 3 | Vendor Analysis | Compares the number of trips by vendor |
| 4 | Passenger Distribution | Analyzes trips by passenger count |
| 5 | Daily Trips | Counts taxi trips for each pickup date |

## Key Findings

### 1. Trips by Hour
- Peak pickup hour: **6 PM (18:00)**
- Trips during peak hour: **90,600**
- Lowest pickup hour: **5 AM (05:00)**
- Trips during lowest hour: **15,002**

### 2. Average Trip Duration
- Longest average duration: **3 PM (15:00)**
- Average duration: **1,118.83 seconds**
- Shortest average duration: **6 AM (06:00)**
- Average duration: **797.43 seconds**

Only positive trip durations were included in this calculation.

### 3. Vendor Analysis
- Vendor 1: **678,342 trips**
- Vendor 2: **780,302 trips**
- Vendor 2 recorded more trips.

### 4. Passenger Distribution
- One passenger: **1,033,540 trips**
- Two passengers: **210,318 trips**
- Most trips involved one passenger.

### 5. Daily Trips
- Total dates analyzed: **182**
- Start date: **January 1, 2016**
- End date: **June 30, 2016**

## Results and Visualizations

The `Charts/` folder contains five visualizations generated using Python and Matplotlib. The `Results/` folder contains the corresponding MapReduce output files.

## Hadoop Execution

The project used Hadoop HDFS for input data storage and Hadoop MapReduce for processing.

All five MapReduce jobs completed successfully. The execution environment used Hadoop's `LocalJobRunner` in Google Colab with a single-node HDFS setup. This project demonstrates Hadoop MapReduce processing but does not represent a multi-node cluster deployment.

## Limitations

- The project used a single-node environment rather than a multi-node Hadoop cluster.
- The analysis is based on the 2016 dataset.
- The dataset may contain unusual or invalid values that require further cleaning.
- The analysis focuses on aggregate patterns rather than individual trip prediction.

## Future Enhancements

- Implement the project on a multi-node Hadoop cluster.
- Analyze pickup and drop-off geographical patterns.
- Add more advanced statistical analysis.
- Compare MapReduce performance with Apache Spark.
- Develop an interactive dashboard.

## References

1. Risdal, M. (2017). *New York City Taxi Trip Duration* [Data set]. Kaggle. https://www.kaggle.com/competitions/nyc-taxi-trip-duration/data
2. Dean, J., & Ghemawat, S. (2008). MapReduce: Simplified data processing on large clusters. *Communications of the ACM, 51*(1), 107–113. https://doi.org/10.1145/1327452.1327492
3. Apache Software Foundation. (n.d.). *Apache Hadoop documentation*. https://hadoop.apache.org/docs/

---

**Project developed as part of Programming for Big Data coursework.**
