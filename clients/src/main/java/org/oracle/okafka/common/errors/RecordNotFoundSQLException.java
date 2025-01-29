package org.oracle.okafka.common.errors;

import java.sql.SQLException;

/** 
 * If a SQL query/PLSQL procedure unexpectedly returns empty result then this exception is thrown.
 */
public class RecordNotFoundSQLException extends SQLException {
	public RecordNotFoundSQLException(String message) {
        super(message);
    }
}
