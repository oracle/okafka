package org.oracle.okafka.tests;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.junit.BeforeClass;

public class OkafkaSetup {
	
    @BeforeClass
    public static Properties setup(){
     
	  final Properties BaseProperties = new Properties();
	  InputStream input;
    	  try {
    		  input = new FileInputStream("src/test/java/test.config");
              BaseProperties.load(input);
	        } catch (Exception e) {
	        	System.out.println("Exception whlie loading config.properties file. " + e);
	        	e.printStackTrace();
	        }
	  return BaseProperties;
    }
}
