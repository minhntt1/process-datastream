package com.example.datastream;

import com.example.datastream.consumer.TraceConsumer;
import com.example.datastream.repo.TimeDataRepo;
import com.example.datastream.service.TimeDataService;

public class Main {
	public static void main(String[] args) {
		final TimeDataRepo cassandra = new TimeDataRepo(
				"cosmosdb111.cassandra.cosmos.azure.com", 
				10350, 
				"cosmosdb111",
				"9EahGOPVoGbaB6XO9OKzsn93C80sZaqUwsVXuJBFh968xlBouJ94Y9lamZYiVzuc3eX8XxCv6Rt8ACDbSmKQ1A=="
		);

		//5 mins bucket
		final TimeDataService timeDataService = new TimeDataService(
				(long) 3e11, 
						cassandra
		);

		final TraceConsumer consumer = new TraceConsumer(
				timeDataService, 
				"dory.srvs.cloudkafka.com:9094", 
				"beqdphse",
				"qkJYICMPWNKDcqatNRTcjeumKnM2hgQd", 
				"beqdphse-tracesdata", 
			"beqdphse-consumers");
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("closing");
			consumer.stop();
		}));
		
		consumer.consume();
	}
}
