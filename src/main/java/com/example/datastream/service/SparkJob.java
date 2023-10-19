package com.example.datastream.service;

//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//public class SparkJob {
////	private SparkContext sparkContext;
//	private SparkSession session;
//	
//	public SparkJob(String host, String port, String user, String pass) {
//		// TODO Auto-generated constructor stub
//		SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster("local[*]");
//        sparkConf.setAppName("spark-job");
//		sparkConf.set("spark.cassandra.connection.host", host);
//		sparkConf.set("spark.cassandra.connection.port", port);
//		sparkConf.set("spark.cassandra.auth.username", user);
//		sparkConf.set("spark.cassandra.auth.password", pass);
//		sparkConf.set("spark.cassandra.input.consistency.level", "ONE");
//		sparkConf.set("spark.cassandra.connection.ssl.enabled", "true");
//		this.session = SparkSession.builder().config(sparkConf).getOrCreate();
////		this.sparkContext = new SparkContext(sparkConf);
//	}
//	
//	public void readData() {
//		Dataset<Row> data = this.session.read()
//		        .format("org.apache.spark.sql.cassandra")
//		        .option("keyspace", "jaeger_v1_dc1")
//		        .option("table", "service_names")
//		        .load();
//		
//		data.show();
//	}
//}
