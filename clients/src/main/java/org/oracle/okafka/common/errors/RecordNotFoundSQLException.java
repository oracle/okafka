package org.oracle.okafka.common.errors;

import java.sql.SQLException;

public class RecordNotFoundSQLException extends SQLException {
	public RecordNotFoundSQLException(String message) {
        super(message);
    }
}
