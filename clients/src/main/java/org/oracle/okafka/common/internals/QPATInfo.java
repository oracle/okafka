package org.oracle.okafka.common.internals;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleData;
import oracle.jdbc.OracleDataFactory;
import oracle.jdbc.OracleStruct;
import oracle.jdbc.internal.ObjectData;
import oracle.jdbc.internal.OracleTypes;
import oracle.jpub.runtime.OracleDataMutableStruct;
public class QPATInfo implements OracleData, OracleDataFactory, ObjectData {
	public static final String _SQL_NAME = "SYS.AQ$_QPAT_INFO";
	public static final int _SQL_TYPECODE = OracleTypes.STRUCT;
	static int[] _sqlType =
	{
	  12, 12, 4, 12, 4, 4, 4, 4, 4, 4, 4, 4, -101
	};
	static OracleDataFactory[] _factory = new OracleDataFactory[13];
	public static QPATInfo _QPATInfo_Factory = new QPATInfo();
	
	OracleDataMutableStruct _struct;
	
	private OracleConnection con = null;
	public static OracleDataFactory getFactory() {
	    return _QPATInfo_Factory;
	  }
	
	public QPATInfo() {
		_struct = new OracleDataMutableStruct(new Object[13], _sqlType, _factory);
	}
	public String name;
	@Override
	public OracleData create(Object d, int sqlType) throws SQLException {
		if (d == null) return null;
	    QPATInfo o = new QPATInfo();
	    if( d instanceof QPATInfo){
	        o.shallowCopy((QPATInfo)d);
	    }else{
	        o._struct = new OracleDataMutableStruct((OracleStruct) d, _sqlType, _factory);
	    }
	    return o;
	}
	@Override
	public Object toJDBCObject(Connection con) throws SQLException {
		Object[] attrbs = new Object[13];
		attrbs[0] = getSchema();
		attrbs[1] = getQueueName();
		attrbs[2] = getQueueId();
		attrbs[3] = getSubscriberName();
		attrbs[4] = getSubscriberId();
		attrbs[5] = getSessionId();
	    attrbs[6] = getGroupLeader();
	    attrbs[7] = getPartitionId();
	    attrbs[8] = getFlags();
	    attrbs[9] = getVersion();
	    attrbs[10] = getInstId();
	    attrbs[11] = getAuditId();
	    attrbs[12] = getOracleTimeStamp();
		return con.createStruct(_SQL_NAME, attrbs);
	}
	public String toString()
	{
		try {
			if(_struct != null)
			{
				String str = "{ Schema:" + getSchema()+",Topic:"+getQueueName()+",ConsumerGroupId:"+getSubscriberName()+
					",GroupLeader:"+getGroupLeader()+",Partition:"+getPartitionId()+",Version"+getVersion()+
					",Flags:"+getFlags()+"}";
				return str;
			
			}
		}catch(Exception e)
		{
			System.out.println("Exception from toString in QPATINFO " + e.getMessage());
			e.printStackTrace();
			return "NULL " + e.getMessage();	
		}
		return "NULL";
		
	}
	
	void shallowCopy(QPATInfo d) throws SQLException {
	    _struct = d._struct;
	  }
	public void setSchema(String schema) throws SQLException{
		_struct.setAttribute(0, schema);
	}
	
	public String getSchema() throws SQLException{
		return (String)_struct.getAttribute(0);
	}
	
	public void setQueueName(String name) throws SQLException{
		_struct.setAttribute(1, name);
	}
	
	public String getQueueName() throws SQLException{
		return (String)_struct.getAttribute(1);
	}
	
	public void setQueueId(int id) throws SQLException{
		_struct.setAttribute(2, new BigDecimal(id));
	}
	
	public Integer getQueueId() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(2)).intValue();
	}
	
	public void setSubscriberName(String name) throws SQLException{
		_struct.setAttribute(3, name);
	}
	
	public String getSubscriberName() throws SQLException{
		return (String)_struct.getAttribute(3);
	}
	
	public void setSubscriberId(int id) throws SQLException{
		_struct.setAttribute(4, new BigDecimal(id));
	}
	
	public Integer getSubscriberId() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(4)).intValue();
	}
	
	public void setSessionId(long session) throws SQLException{
		_struct.setAttribute(5, new BigDecimal(session));
	}
	
	public Long getSessionId() throws SQLException{
		return (Long)((BigDecimal)_struct.getAttribute(5)).longValue();
	}
	
	public void setGroupLeader(int leader) throws SQLException{
		_struct.setAttribute(6, new BigDecimal(leader));
	}
	
	public Integer getGroupLeader() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(6)).intValue();
	}
	
	public void setPartitionId(int partition) throws SQLException{
		_struct.setAttribute(7, new BigDecimal(partition));
	}
	
	public Integer getPartitionId() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(7)).intValue();
	}
	
	public void setFlags(int flags) throws SQLException {
		_struct.setAttribute(8, new BigDecimal(flags));
	}
	
	public Integer getFlags() throws SQLException {
			if((BigDecimal)_struct.getAttribute(8) == null)
			{
				return 0;
			}
			return (Integer)((BigDecimal)_struct.getAttribute(8)).intValue();
	}
	
	public void setVersion(int version) throws SQLException{
		_struct.setAttribute(9, new BigDecimal(version));
	}
	
	public Integer getVersion() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(9)).intValue();
	}
	
	public void setInstId(int inst) throws SQLException{
		_struct.setAttribute(10, new BigDecimal(inst));
	}
	
	public Integer getInstId() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(10)).intValue();
	}
	
	public void setAuditId(long auditId) throws SQLException{
		_struct.setAttribute(11, new BigDecimal(auditId));
	}
	
	public Long getAuditId() throws SQLException{
		return (Long)((BigDecimal)_struct.getAttribute(11)).longValue();
	}
	
	public void setTimeStamp(java.sql.Time time) throws SQLException{
		if(con != null)
		  _struct.setAttribute(12, new oracle.sql.TIMESTAMPTZ(con, time));
		else _struct.setAttribute(12, new oracle.sql.TIMESTAMPTZ());
	}
	
	public java.sql.Time getTimeStamp() throws SQLException{
		return (java.sql.Time)((oracle.sql.TIMESTAMPTZ)_struct.getAttribute(12)).timeValue();
	}
	
	private oracle.sql.TIMESTAMPTZ getOracleTimeStamp() throws SQLException {
		return (oracle.sql.TIMESTAMPTZ)_struct.getAttribute(12);
		
	}
	
	public void setConnection(OracleConnection conn)
	{
		con = conn;
	}
	
	public OracleConnection getConnection() {
		return con;
	}
}