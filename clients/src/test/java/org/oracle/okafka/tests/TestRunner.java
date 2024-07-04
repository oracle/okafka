package org.oracle.okafka.tests;
import java.util.Scanner;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;


class TestRunner {
     
	public static void main(String[] args) {
		
		Result result = new Result();
	
		result = JUnitCore.runClasses(SimpleOkafkaAdmin.class, SimpleOkafkaProducer.class, OkafkaAutoOffsetReset.class,
				  SimpleOkafkaProducer.class, OkafkaSeekToEnd.class, OkafkaSeekToBeginning.class, SimpleOkafkaProducer.class,
				  OkafkaUnsubscribe.class, OkafkaDeleteTopic.class,ProducerMetricsTest.class, ConsumerMetricsTest.class);
		
		for (Failure failure : result.getFailures()) {
	        System.out.println("Test failure : "+ failure.toString());
	    }
			System.out.println("Tests ran succesfully: " + result.wasSuccessful());
		    
	   }
   }

