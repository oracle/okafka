package org.oracle.okafka.common.requests;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.protocol.ApiKeys;

public class ConnectMeResponse extends AbstractResponse {

	class DBListener
	{
		String protocol;
		String host;
		int port;
		boolean local;
	}
	
	int instId;
	String instanceName;
	int flags;
	String url;
	ArrayList<String> serviceNames;
	ArrayList<String> localListeners;
	ArrayList<Integer> partitionList;
	ArrayList<DBListener> dbListenerList;
	ArrayList<Node> nodeList;
	Node preferredNode;
	
	public ConnectMeResponse()
	{
		super(ApiKeys.CONNECT_ME);
		partitionList = new ArrayList<Integer>();
	}

	public int getInstId() {
		return instId;
	}
	public void setInstId(int instId) {
		this.instId = instId;
	}
	public String getInstanceName() {
		return instanceName;
	}
	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}
	public int getFlags() {
		return flags;
	}
	public void setFlags(int flags) {
		this.flags = flags;
	}
	public String getUrl() {
		return url;
	}
	public Node getPreferredNode()
	{
		return preferredNode;
	}
	public void setUrl(String url) {
		this.url = url;
	
	}
	public void setPreferredNode(Node _node)
	{
		this.preferredNode = _node;
	}
	

	public ArrayList<Node> processUrl()
	{
		/* Expected Url is in JSON format. Below is one example:
		 * {"INSTANCE_NAME":"v1","SERVICE_NAME":["CDB1_PDB1_I1.regress.rdbms.dev.us.oracle.com","cdb1_pdb1.regress.rdbms.dev.us.oracle.com"],"LOCAL_LISTENER":["(ADDRESS=(PROTOCOL=ipc)(KEY=v1))","(ADDRESS=(PROTOCOL=tcp)(HOST=phoenix94147)(PORT=1521))"],"NEW_PARA":"NEW_PARA_VALUE","NEW_PARA_2":["VALUE 1","Value 2"]}
		 * */

		if(url == null)
			return nodeList;
		
		String strippedUrl = url.replace('{',' ');
		strippedUrl = strippedUrl.replace('}',' ');
		strippedUrl = strippedUrl.trim();

		ArrayList<String> values = new ArrayList<String>();

		StringTokenizer stn = new StringTokenizer(strippedUrl,":,[]", true);
		while(stn.hasMoreTokens())
		{
			String tokenNow = stn.nextToken();
			String seperator = stn.nextToken();
			//System.out.println("Token Now " + tokenNow +" Seperator " + seperator);
			String tokenName, tokenValue;
			boolean multiples = false;
			if(seperator.equals(":"))
			{
				tokenNow = tokenNow.trim();
				tokenName = tokenNow.substring(1,tokenNow.length()-1);
				multiples = false;
				// Next token decides if this is a stand-alone value or multiples
				tokenValue = stn.nextToken();
				if(tokenValue.equals("["))
				{
					boolean valuesDone = false;
					multiples = true;
					values.clear();
					do {
						tokenValue = stn.nextToken();
						seperator = stn.nextToken();
						//System.out.println("Multiples: TokenValue : " + tokenValue + " seperator= " + seperator);
						if(seperator.equals("]"))
						{
							valuesDone = true;
							if(stn.hasMoreTokens())
							{
								String endDelim = stn.nextToken();
								//System.out.println("Multiples : ENd Delim " + endDelim);
							}
						}
						tokenValue = tokenValue.trim();
						tokenValue = tokenValue.substring(1,tokenValue.length()-1);
						values.add(tokenValue);
					}while(!valuesDone && stn.hasMoreTokens());
				}
				else  // Single Value
				{
					tokenValue = tokenValue.trim();
					tokenValue = tokenValue.substring(1,tokenValue.length()-1);

					//System.out.println("Token KEY= '"+tokenName+"' Token Value ='" + tokenValue);
					if(stn.hasMoreTokens())
					{
						seperator = stn.nextToken();
					}
				}

				//System.out.println("TokenName " + tokenName);
				if(tokenName.equalsIgnoreCase("INSTANCE_NAME"))
				{
					if(!multiples)
					{
						instanceName = tokenValue;
					}
				}
				else if(tokenName.equalsIgnoreCase("SERVICE_NAME"))
				{
					serviceNames = new ArrayList<String>();
					if(!multiples)
					{
						serviceNames.add(tokenValue);
					}
					else {
						serviceNames.addAll(values);
					}
				}
				else if(tokenName.equalsIgnoreCase("LOCAL_LISTENER"))
				{
					localListeners = new ArrayList<String>();
					if(!multiples)
					{
						localListeners.add(tokenValue);
					}
					else {
						localListeners.addAll(values);
					}
				}
			}
		}
		/*		System.out.println("Instance Name " + instanceName);
		if(serviceNames != null)
		{
			for(String service: serviceNames)
			{
				System.out.println("Service " + service);
			}
		}
		*/
		
		if(localListeners != null)
		{
			dbListenerList = new ArrayList<DBListener>(localListeners.size());
			for(String listener: localListeners)
			{
				DBListener dbListener = parseLocalListener(listener);
				if(dbListener != null)
					dbListenerList.add(dbListener);
			}
			prepareNodeList();
		}
		
		return nodeList;
	}
	//Ad-Hoc processing of LISTENER STRING 
	private DBListener parseLocalListener(String listener)
	{
		DBListener dbListener = null;
		try {
			dbListener = new DBListener();
			String str = listener;
			
			StringBuilder sb = new StringBuilder();
			for(int ind = 0;ind < str.length(); ind++)
				if(str.charAt(ind) != ' ')
					sb.append(str.charAt(ind));
			str = sb.toString();
			String protocol = AQClient.getProperty(str,"PROTOCOL");
			String host = AQClient.getProperty(str, "HOST");;
			Integer port = Integer.parseInt(AQClient.getProperty(str, "PORT"));

			dbListener.host = host;
			dbListener.port = port;
			dbListener.protocol = protocol;
		}catch(Exception e )
		{
		}
		return dbListener;
	}
	
	private void prepareNodeList()
	{
		if(dbListenerList == null || serviceNames == null)
			return;
		
		if(nodeList == null)
			nodeList = new ArrayList<Node>();
		else 
			nodeList.clear();
		
		for(DBListener listenerNow: dbListenerList)
		{
			if(listenerNow.protocol == null)
				continue;
			
			if(listenerNow.protocol.equalsIgnoreCase("TCP") || listenerNow.protocol.equalsIgnoreCase("TCPS"))
			{
				for(String servcieName: serviceNames)
				{
					Node newNode = new Node(instId, listenerNow.host, listenerNow.port, servcieName, instanceName);
					newNode.setProtocol(listenerNow.protocol);
					nodeList.add(newNode);
				}
			}
		}
	}
	
	public ArrayList<String> getServiceNames() {
		return serviceNames;
	}
	public void setServiceNames(ArrayList<String> serviceNames) {
		this.serviceNames = serviceNames;
	}
	public ArrayList<String> getLocalListeners() {
		return localListeners;
	}
	public void setLocalListeners(ArrayList<String> localListeners) {
		this.localListeners = localListeners;
	}
	public ArrayList<Integer> getPartitionList() {
		return partitionList;
	}
	public void setPartitionList(BigDecimal[] partitionArray) {
		this.partitionList.clear();
		for(BigDecimal pId : partitionArray)
		{
			this.partitionList.add(pId.intValue());
		}
	}

	public void setPartitionList(ArrayList<Integer> partitionList) {
		this.partitionList = partitionList;
	}

	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Errors, Integer> errorCounts() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int throttleTimeMs() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void maybeSetThrottleTimeMs(int arg0) {
		// TODO Auto-generated method stub
		
	}


}
