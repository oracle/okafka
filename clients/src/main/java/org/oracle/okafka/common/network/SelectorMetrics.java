package org.oracle.okafka.common.network;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.oracle.okafka.common.Node;

public class SelectorMetrics {

	private final Metrics metrics;
    private final Map<String, String> metricTags;
    private final boolean metricsPerConnection;
    private final String metricGrpName;
    private final String perConnectionMetricGrpName;
    
    public final Sensor connectionClosed;
    public final Sensor connectionCreated;
    public final Sensor requestsSent;
    public final Sensor responsesReceived;
    
    
    /* Names of metrics that are not registered through sensors */
    private final List<MetricName> topLevelMetricNames = new ArrayList<>();
    private final List<Sensor> sensors = new ArrayList<>();

	public SelectorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> metricTags,boolean metricsPerConnection) {

		this.metrics = metrics;
        this.metricTags = metricTags;
        this.metricsPerConnection = metricsPerConnection;
        this.metricGrpName = metricGrpPrefix + "-metrics";
        this.perConnectionMetricGrpName = metricGrpPrefix + "-node-metrics";
        
        StringBuilder tagsSuffix = new StringBuilder();

        for (Map.Entry<String, String> tag: metricTags.entrySet()) {
            tagsSuffix.append(tag.getKey());
            tagsSuffix.append("-");
            tagsSuffix.append(tag.getValue());
        }

		this.connectionClosed = sensor("connections-closed:" + tagsSuffix);
		this.connectionClosed.add(createMeter(metrics, metricGrpName, metricTags,
				"connection-close", "connections closed"));

		this.connectionCreated = sensor("connections-created:" + tagsSuffix);
		this.connectionCreated.add(createMeter(metrics, metricGrpName, metricTags,
				"connection-creation", "new connections established"));

		this.requestsSent = sensor("requests-sent:" + tagsSuffix);
		this.requestsSent.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(), "request", "requests sent"));
		MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", metricTags);
		this.requestsSent.add(metricName, new Avg());
		metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", metricTags);
		this.requestsSent.add(metricName, new Max());


		this.responsesReceived = sensor("responses-received:" + tagsSuffix);
		this.responsesReceived.add(createMeter(metrics, metricGrpName, metricTags,
				new WindowedCount(), "response", "responses received"));
		
		/*
		metricName=metrics.metricName("connection-count", metricGrpName,"The current number of active connections.", metricTags);
		topLevelMetricNames.add(metricName);
		this.metrics.addMetric(metricName, (config, now) -> topicPublishersMap.size());
		 */
	}

	private Sensor sensor(String name, Sensor... parents) {
		Sensor sensor = metrics.sensor(name, parents);
		sensors.add(sensor);
		return sensor;
	}

	private Meter createMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
			String baseName, String descriptiveName) {
		return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName);
	}

	private Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags, SampledStat stat,
			String baseName, String descriptiveName) {
		MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
				String.format("The number of %s per second", descriptiveName), metricTags);
		MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
				String.format("The total number of %s", descriptiveName), metricTags);
		if (stat == null)
			return new Meter(rateMetricName, totalMetricName);
		else
			return new Meter(stat, rateMetricName, totalMetricName);
	}
	
	public void recordConnectionCount(Map<Node,?> map ) {
		MetricName metricName=metrics.metricName("connection-count", metricGrpName,"The current number of active connections.", metricTags);
		topLevelMetricNames.add(metricName);
		this.metrics.addMetric(metricName, (config, now) -> map.size());
	}

	public void maybeRegisterConnectionMetrics(Node node) {


		if (!node.isEmpty() && metricsPerConnection) {
			// if one sensor of the metrics has been registered for the connection,
			// then all other sensors should have been registered; and vice versa
			String connectionId="" +node.id();
			String nodeRequestName = "node-" + connectionId + ".requests-sent";
			Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
			Sensor nodeDescription=sensor("node-description:");
			MetricName  metricName= metrics.metricName("Node-"+ node.id(), metricGrpName, node.toString(), metricTags);
			nodeDescription.add(metricName,new WindowedCount());
			if (nodeRequest == null) {
				Map<String, String> tags = new LinkedHashMap<>(metricTags);
				tags.put("node-id", "node-" + connectionId);

				nodeRequest = sensor(nodeRequestName);
				nodeRequest.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "request", "requests sent"));
				metricName = metrics.metricName("request-size-avg", perConnectionMetricGrpName, "The average size of requests sent.", tags);
				nodeRequest.add(metricName, new Avg());
				metricName = metrics.metricName("request-size-max", perConnectionMetricGrpName, "The maximum size of any request sent.", tags);
				nodeRequest.add(metricName, new Max());

				String bytesSentName = "node-" + connectionId + ".bytes-sent";
				Sensor bytesSent = sensor(bytesSentName);
				bytesSent.add(createMeter(metrics, perConnectionMetricGrpName, tags, "outgoing-byte", "outgoing bytes"));

				String nodeResponseName = "node-" + connectionId + ".responses-received";
				Sensor nodeResponse = sensor(nodeResponseName);
				nodeResponse.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "response", "responses received"));

				String bytesReceivedName = "node-" + connectionId + ".bytes-received";
				Sensor bytesReceive = sensor(bytesReceivedName);
				bytesReceive.add(createMeter(metrics, perConnectionMetricGrpName, tags, "incoming-byte", "incoming bytes"));

				String nodeTimeName = "node-" + connectionId + ".latency";
				Sensor nodeRequestTime = sensor(nodeTimeName);
				metricName = metrics.metricName("request-latency-avg", perConnectionMetricGrpName, tags);
				nodeRequestTime.add(metricName, new Avg());
				metricName = metrics.metricName("request-latency-max", perConnectionMetricGrpName, tags);
				nodeRequestTime.add(metricName, new Max());
			}
		}
	}
	
	 public void requestCompletedSend(String connectionId) {
            requestsSent.record();
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record();
               
            }
        }

	public void recordCompletedSend(String connectionId, long totalBytes, long currentTimeMs) {
		requestsSent.record(totalBytes, currentTimeMs, false);
		if (!connectionId.isEmpty()) {
			String nodeRequestName = "node-" + connectionId + ".requests-sent";
			Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
			if (nodeRequest != null) {
				nodeRequest.record(totalBytes, currentTimeMs);
			}
		}
	}
	
	public void recordCompletedReceive(String connectionId,double latencyMs) {
        responsesReceived.record();
        if (!connectionId.isEmpty()) {
            String nodeRequestName = "node-" + connectionId + ".responses-received";
            Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
            if (nodeRequest != null)
                nodeRequest.record();
            String nodeTimeName="node-" + connectionId + ".latency";
            Sensor nodeRequestTime=this.metrics.getSensor(nodeTimeName);
            if(nodeRequestTime!=null) 
            	nodeRequestTime.record(latencyMs);
        }
    }

	public void close() {
		for (MetricName metricName : topLevelMetricNames)
			metrics.removeMetric(metricName);
		for (Sensor sensor : sensors)
			metrics.removeSensor(sensor.name());

	}
}
