/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.errors;

import org.apache.kafka.common.KafkaException;

/** 
 * Thrown when OKafka application fails to connect with the Oracle Database. 
 */
public class ConnectionException extends KafkaException {
	private static final long serialVersionUID = 1L;

	public ConnectionException(Throwable cause) {
		super(cause);
	}
	public ConnectionException(String msg) {
		super(msg);
	}
	
	public ConnectionException(String msg, Throwable cause) {
		super(msg, cause);
	}
	
	public ConnectionException() {
		super();
	}
 }
