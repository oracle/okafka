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
public class QPIMInfo implements OracleData, OracleDataFactory, ObjectData {
	public static final String _SQL_NAME = "SYS.AQ$_QPIM_INFO";
	public static final int _SQL_TYPECODE = OracleTypes.STRUCT;
	static int[] _sqlType =
	{
	  12, 12, 4, 4
	};
	static OracleDataFactory[] _factory = new OracleDataFactory[4];
	public static QPIMInfo _QPIMInfo_Factory = new QPIMInfo();
	
	OracleDataMutableStruct _struct;
	
	private OracleConnection con = null;
	public static OracleDataFactory getFactory() {
	    return _QPIMInfo_Factory;
	  }
	
	public QPIMInfo() {
		_struct = new OracleDataMutableStruct(new Object[4], _sqlType, _factory);
	}
	@Override
	public OracleData create(Object d, int sqlType) throws SQLException {
		if (d == null) return null;
	    QPIMInfo o = new QPIMInfo();
	    if( d instanceof QPIMInfo){
	        o.shallowCopy((QPIMInfo)d);
	    }else{
	        o._struct = new OracleDataMutableStruct((OracleStruct) d, _sqlType, _factory);
	    }
	    return o;
	}
	@Override
	public Object toJDBCObject(Connection con) throws SQLException {
		
		Object[] attrbs = new Object[13];
		attrbs[0] = getOwner();
		attrbs[1] = getQueueName();
		attrbs[2] = getPartitionId();
		attrbs[3] = getOwnerInstId();
		return con.createStruct(_SQL_NAME, attrbs);
	}
	
	public String toString()
	{
		if(_struct == null)
			return null;
		try
		{
			String str = "{OwnerInstance:"+getOwner()+",TopicName:"+getQueueName()+",Partition:"+getPartitionId()+",OwnerInstanceID:"+getOwnerInstId()+"}";
			return str;
		}catch(Exception e)
		{
			return "Null " +e.getMessage();
		}
	}
	
	void shallowCopy(QPIMInfo d) throws SQLException {
	    _struct = d._struct;
	  }
	public void setOwner(String owner) throws SQLException{
		_struct.setAttribute(0, owner);
	}
	
	public String getOwner() throws SQLException{
		return (String)_struct.getAttribute(0);
	}
	
	public void setQueueName(String name) throws SQLException{
		_struct.setAttribute(1, name);
	}
	
	public String getQueueName() throws SQLException{
		return (String)_struct.getAttribute(1);
	}
	public void setPartitionId(int partition) throws SQLException{
		_struct.setAttribute(2, new BigDecimal(partition));
	}
	
	public Integer getPartitionId() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(2)).intValue();
	}
	
	public void setOwnerInstId(int inst) throws SQLException{
		_struct.setAttribute(3, new BigDecimal(inst));
	}
	
	public Integer getOwnerInstId() throws SQLException{
		return (Integer)((BigDecimal)_struct.getAttribute(3)).intValue();
	}
}