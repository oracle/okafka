package org.oracle.okafka.common.errors;

import java.sql.SQLException;

/** 
 * Exception indicates that either specified topic name/id in describeTopics()/deleteTopic() call is not found.
 */
public class RecordNotFoundSQLException extends SQLException {
	public RecordNotFoundSQLException(String message) {
        super(message);
    }
}
