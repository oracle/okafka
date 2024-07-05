package org.oracle.okafka.common.internals;
import java.sql.Connection;
import java.sql.SQLException;
import org.oracle.okafka.common.internals.QPATInfo;
import oracle.jdbc.OracleArray;
import oracle.jdbc.OracleData;
import oracle.jdbc.OracleDataFactory;
import oracle.jdbc.internal.OracleTypes;
import oracle.jpub.runtime.OracleDataMutableArray;
public class QPATInfoList implements OracleData, OracleDataFactory {
    public static final String _SQL_NAME = "SYS.AQ$_QPAT_INFO_LIST";
    public static final int _SQL_TYPECODE = OracleTypes.ARRAY;
    OracleDataMutableArray _array;
    private static final QPATInfoList _QPATInfoList_Factory = new QPATInfoList();
	
    public static OracleDataFactory getOracleDataFactory() {
    	return _QPATInfoList_Factory;
    }
    public QPATInfoList()
    {
      this((QPATInfo[])null);
    }
    public QPATInfoList(QPATInfo[] a)
    {
      _array = new OracleDataMutableArray(2002, a, QPATInfo.getFactory());
    }
    @Override
	public OracleData create(Object d, int sqlType) throws SQLException {
    	if (d == null) return null;
        QPATInfoList a = new QPATInfoList();
        a._array = new OracleDataMutableArray(2002, (OracleArray) d, QPATInfo.getFactory());
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
	  public QPATInfo[] getArray() throws SQLException
	  {
	    return (QPATInfo[]) _array.getObjectArray(
	      new QPATInfo[_array.length()]);
	  }
	  public void setArray(QPATInfo[] a) throws SQLException
	  {
	    _array.setObjectArray(a);
	  }
	  public QPATInfo[] getArray(long index, int count) throws SQLException
	  {
	    return (QPATInfo[]) _array.getObjectArray(index,
	      new QPATInfo[_array.sliceLength(index, count)]);
	  }
	  public void setArray(QPATInfo[] a, long index) throws SQLException
	  {
	    _array.setObjectArray(a, index);
	  }
	  public QPATInfo getElement(long index) throws SQLException
	  {
	    return (QPATInfo) _array.getObjectElement(index);
	  }
	  public void setElement(QPATInfo a, long index) throws SQLException
	  {
	    _array.setObjectElement(a, index);
	  }
}