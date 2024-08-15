package org.oracle.okafka.common.internals;
import java.sql.Connection;
import java.sql.SQLException;
import org.oracle.okafka.common.internals.QPIMInfo;
import oracle.jdbc.OracleArray;
import oracle.jdbc.OracleData;
import oracle.jdbc.OracleDataFactory;
import oracle.jdbc.internal.OracleTypes;
import oracle.jpub.runtime.OracleDataMutableArray;
public class QPIMInfoList implements OracleData, OracleDataFactory {
    public static final String _SQL_NAME = "SYS.AQ$_QPIM_INFO_LIST";
    public static final int _SQL_TYPECODE = OracleTypes.ARRAY;
    OracleDataMutableArray _array;
    private static final QPIMInfoList _QPIMInfoList_Factory = new QPIMInfoList();
	
    public static OracleDataFactory getOracleDataFactory() {
    	return _QPIMInfoList_Factory;
    }
    public QPIMInfoList()
    {
      this((QPIMInfo[])null);
    }
    public QPIMInfoList(QPIMInfo[] a)
    {
      _array = new OracleDataMutableArray(2002, a, QPIMInfo.getFactory());
    }
    @Override
	public OracleData create(Object d, int sqlType) throws SQLException {
    	if (d == null) return null;
        QPIMInfoList a = new QPIMInfoList();
        a._array = new OracleDataMutableArray(2002, (OracleArray) d, QPIMInfo.getFactory());
        return a;
	}
	@Override
	public Object toJDBCObject(Connection con) throws SQLException {
		return _array.toJDBCObject(con, _SQL_NAME);
	}
	
	public int length() throws SQLException {
	  return _array.length();
	}
	public int getBaseType() throws SQLException{
	  return _array.getBaseType();
    }
	  public String getBaseTypeName() throws SQLException
	  {
	    return _array.getBaseTypeName();
	  }
	  public QPIMInfo[] getArray() throws SQLException
	  {
	    return (QPIMInfo[]) _array.getObjectArray(
	      new QPIMInfo[_array.length()]);
	  }
	  public void setArray(QPIMInfo[] a) throws SQLException
	  {
	    _array.setObjectArray(a);
	  }
	  public QPIMInfo[] getArray(long index, int count) throws SQLException
	  {
	    return (QPIMInfo[]) _array.getObjectArray(index,
	      new QPIMInfo[_array.sliceLength(index, count)]);
	  }
	  public void setArray(QPIMInfo[] a, long index) throws SQLException
	  {
	    _array.setObjectArray(a, index);
	  }
	  public QPIMInfo getElement(long index) throws SQLException
	  {
	    return (QPIMInfo) _array.getObjectElement(index);
	  }
	  public void setElement(QPIMInfo a, long index) throws SQLException
	  {
	    _array.setObjectElement(a, index);
	  }
	
}