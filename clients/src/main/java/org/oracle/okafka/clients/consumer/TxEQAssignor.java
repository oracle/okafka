/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
//import org.oracle.okafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.oracle.okafka.common.internals.PartitionData;
import org.oracle.okafka.common.internals.SessionData;

/**
 * With Oracle Transactional Event Queue (TxEQ) and in Oracle RAC environment, different partitions of a Topic can be owned
 * by different RAC instances. To get maximum throughput while consuming messages from these partitions, 
 * best case is to consume messages from local partitions. That means, if a session 1 is connected to Oracle RAC instance 1,
 * then it is preferable to assign partitions owned by instance 1 to session 1. 
 * If session 1 is assigned a topic-partition owned by instance 2, then consuming messages from these 'Remote' partition
 * involves additional overhead and slows down the performance. 
 * 
 * {@link TxEQAssignor#assign(org.apache.kafka.common.Cluster, org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription)}
 * method tries to distribute partitions so that, preference is given to Local assignment.
 * This follows below logic for assigning partitions.
 * 
 * 1. Equal and fair distribution of partitions among alive consumer sessions
 * 2. 1st Preference for Local Assignment
 * 3. 2nd Preference for Sticky Assignment
 * 
 */

public class TxEQAssignor extends AbstractPartitionAssignor {

	Map<Integer, ArrayList<Integer>> instPListMap ;
	Map<String, ArrayList<SessionData>> partitionMemberMap;

	public void setInstPListMap(Map<Integer, ArrayList<Integer>> _instPListMap)
	{
		instPListMap = _instPListMap;
	}
	
	public void setPartitionMemberMap(Map<String, ArrayList<SessionData>> _partitionMemberMap)
	{
		partitionMemberMap = _partitionMemberMap;
	}
	
	
	
	
	
	// Not Invoked. To be removed
	public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
			Map<String, Subscription> subscriptions,  int oldVersion)
	{
		Map<String, List<TopicPartition>> assignment = new HashMap<>();
		Map<TopicPartition,SessionData> partitionSessionMap = new HashMap<TopicPartition, SessionData>();
		Map<Integer, ArrayList<SessionData>> instSessionDataMap = new HashMap<Integer, ArrayList<SessionData>>();
		
		//System.out.println("TxEQAssignor 1:Creating assignment map for each member ");
		for (String memberId : subscriptions.keySet())
		{
			//System.out.println("TxEQAssignor 2:MemberID in Assignment Map " + memberId);
			assignment.put(memberId, new ArrayList<TopicPartition>());
			
		}
		
		/* create PartitionId to Session(TMember) mapping */
		for(String topicNow : partitionsPerTopic.keySet())
		{
			ArrayList<SessionData> sDataForTopic =  partitionMemberMap.get(topicNow);
			for(SessionData sDataNow : sDataForTopic)
			{
				ArrayList<SessionData> instSDataList = instSessionDataMap.get(sDataNow.getInstanceId());
				if(instSDataList == null)
				{
					instSDataList = new ArrayList<SessionData>();
					instSessionDataMap.put(sDataNow.getInstanceId(),instSDataList);
				}
				instSDataList.add(sDataNow);
				
				List<PartitionData> previousAssignment = sDataNow.getPreviousPartitions();
				for(PartitionData pDataNow : previousAssignment)
				{
					partitionSessionMap.put(pDataNow.getTopicPartition(),sDataNow);
					
				}
			}
		}
		
		/*
		 * Logic to Assign Partitions:
		 * FixedPartiton = Fixed number of partitions assigned to all sessions
		 * FloatingPartition: Some sessions can be assigned one additional partition. Number of such additional partitions is floatingPartitions
		 * 
		 * Pass 1: FIXED + LOCAL + STICKY
		 *    Sessions to be assigned Fixed number of partitions only. All assignment will be Local and Sticky.
		 * Pass 2: FIXED + LOCAL  
		 * 	Sessions to be assigned Fixed number of partitions only. All assignment will be Local and new assignment ( non sticky).
		 * Pass 3: FLOATING + LOCAL + STICKY
		 *    Some Sessions to be assigned Floating partitions. All assignment will be Local and Sticky.
		 * Pass 4: FLOATING + LOCAL 
		 *    Some Sessions to be assigned Floating partitions. All assignment will be Local and new assignment.
		 * Pass 5: REMOTE + STICKY 
		 *    Some Sessions to be assigned Fixed/Floating partitions. All assignment will be REMOTE and STICKY
		 * Pass 6: REMOTE + NEW 
		 *    Some Sessions to be assigned Fixed/Floating partitions. All assignment will be Remote and new assignment.
		 * 
		 * Example: 
		 *   2 Node RAC. 7 Partitions for a TOPIC T1. Partitions distributed 4 | 3 . i.e. Instance 1 owns 4 and instance 2 owns 3 partitions.
		 *   Current 2 sessions connected to instance 1. 
		 * 
		 * 
		 *  */
		
		
		for(String topicNow : partitionsPerTopic.keySet())
		{
			Map<Integer,Integer> instPCntMap = new HashMap<Integer,Integer>();
			//Calculate how many partitions to be owned by each member
			ArrayList<SessionData> memberList = partitionMemberMap.get(topicNow);

			int membersCnt = memberList.size();
			//System.out.println("TxEQAssignor: Total members to be assigned partitions are " + membersCnt);
			int partitionCnt = partitionsPerTopic.get(topicNow);
			int fixedPartitions = partitionCnt/membersCnt;
			int floatingPartitions = partitionCnt%membersCnt;

			for(SessionData tMem : memberList)
			{
				tMem.pendingCnt = fixedPartitions;
				tMem.oneMore = false;
			}
			
			//Assigning Session to the partition from partition Map
			
			for(Integer instNow : instPListMap.keySet())
			{
				ArrayList<Integer> pInstList = instPListMap.get(instNow);
				ArrayList<Integer> removeList = new ArrayList<Integer>();
				/* Local Sticky Assignment */
				for(Integer pNow : pInstList)
				{
					//System.out.println("TxEQAssignor: Assigning Partition Now from Instance " + pNow + " , " + instNow);
					TopicPartition tpNow = new TopicPartition(topicNow, pNow);
					SessionData tMemNow = partitionSessionMap.get(tpNow);
					if(tMemNow != null)
					{
						// If Locally assigned and previously owned and pending count has not exhausted
						if(tMemNow.getInstanceId() == instNow && ( (tMemNow.pendingCnt > 0) || ( tMemNow.pendingCnt == 0 && !tMemNow.oneMore && floatingPartitions > 0 ) ))
						{
							PartitionData teqP = new PartitionData(topicNow, tMemNow.getQueueId(), pNow, tMemNow.getSubscriberName(), tMemNow.getSubscriberId(),tMemNow.getInstanceId(), true);
							tMemNow.addAssignedPartitions(teqP);
							if(tMemNow.pendingCnt <= 0)
							{
								tMemNow.oneMore = true;
								ArrayList<SessionData> instSDataList =  instSessionDataMap.get(tMemNow.getInstanceId());
								instSDataList.remove(tMemNow);
							}
							else
								tMemNow.pendingCnt--;
							
							/*System.out.println("TxEQAssignor: Partition " + pNow +" Assigned to (" + tMemNow.getSessionId() + ", " + tMemNow.getInstanceId()+"). Pending "
									+" Partition Count for this member " + tMemNow.pendingCnt + " OnMore Assigned ? " + tMemNow.oneMore);*/
							
							removeList.add(pNow);
						}
					}
				}
				pInstList.removeAll(removeList);
				removeList.clear();
				
				/* Local New Assignment */
				for(Integer pNow : pInstList)
				{
					
				}
			}

			
		}
		return assignment;
	}
	
	@Override
	public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
			Map<String, Subscription> subscriptions) {

		Map<String, List<TopicPartition>> assignment = new HashMap<>();

		// System.out.println("TxEQAssignor: TxEQ Assign 1:Creating assignment map for
		// each member ");
		for (String memberId : subscriptions.keySet()) {
			// System.out.println("TxEQAssignor 2:MemberID in Assignment Map " + memberId);
			assignment.put(memberId, new ArrayList<TopicPartition>());
		}

		Map<Integer, ArrayList<SessionData>> instSessionDataMap = new HashMap<Integer, ArrayList<SessionData>>();
		Map<TopicPartition, SessionData> partitionSessionMap = new HashMap<TopicPartition, SessionData>();

		for (String topicNow : partitionsPerTopic.keySet()) {
			instSessionDataMap.clear();
			partitionSessionMap.clear();

			// Calculate how many partitions to be owned by each member
			ArrayList<SessionData> memberList = partitionMemberMap.get(topicNow);
			int membersCnt = memberList.size();

			// Actual Partitions created
			int totalPartitionsCreated = 0;
			for (int instNow : instPListMap.keySet()) {
				ArrayList<Integer> pList = instPListMap.get(instNow);
				if (pList != null) {
					totalPartitionsCreated += pList.size();
				}
			}

			// System.out.println("TxEQAssignor 2.1: Total members to be assigned partitions
			// are " + membersCnt);
			// int partitionCnt = partitionsPerTopic.get(topicNow);

			// Consider total partitions actually created instead of partitions which are
			// supposed to be created
			// TxEQ creates partitions as and when message is published into the topic
			// partitions.
			int partitionCnt = totalPartitionsCreated;

			int partForAll = partitionCnt / membersCnt;
			int partForSome = partitionCnt % membersCnt;
			// System.out.println("TxEQAssignor 2.2: Partitions for All " + partForAll + "
			// Partitions for Some " + partForSome);
			// All sessions will minimally get <partForAll> partitions.
			for (SessionData tMem : memberList) {
				tMem.pendingCnt = partForAll;
			}

			/*
			 * Some sessions will get 1 additional partition. Below logic decides which
			 * sessions should be assigned 1 more partition. Ideally, pick sessions which
			 * can be assigned partition locally.
			 */
			if (partForSome > 0) {
				int maxInst = 0; // MAX Instance id where a partition is owned or a session is connected

				// Prepare map to see how sessions are spread across instances.
				for (SessionData sDataNow : memberList) {
					int instNow = sDataNow.getInstanceId();
					if (instNow > maxInst)
						maxInst = instNow;

					ArrayList<SessionData> instSDataList = instSessionDataMap.get(instNow);
					if (instSDataList == null) {
						instSDataList = new ArrayList<SessionData>();
						instSessionDataMap.put(instNow, instSDataList);
					}
					instSDataList.add(sDataNow);
					// System.out.println("TxEQAssignor: Session count for inst " + instNow + " = "
					// + instSDataList.size());

					/*
					 * List<PartitionData> previousAssignment = sDataNow.getPreviousPartitions();
					 * for(PartitionData pDataNow : previousAssignment) {
					 * partitionSessionMap.put(pDataNow.getTopicPartition(),sDataNow); }
					 */
				}
				for (int instNow : instPListMap.keySet()) {
					if (instNow > maxInst) {
						maxInst = instNow;
					}
				}

				int extraPerInst[] = new int[maxInst];
				int extraPerInstMax[] = new int[maxInst];
				for (int i = 0; i < maxInst; i++) {
					extraPerInst[i] = 0;
					extraPerInstMax[i] = 0;
				}

				for (int instNow : instPListMap.keySet()) {
					// Partitions owned by this instance
					int pCnt = instPListMap.get(instNow).size();

					ArrayList<SessionData> instSessionDataList = instSessionDataMap.get(instNow);
					// Sessions connected to this instance
					int sessionCnt = instSessionDataList != null ? instSessionDataList.size() : 0;
					// Maximum number of extra partitions that can be assigned to this instance
					extraPerInstMax[instNow - 1] = sessionCnt;

					// Number of extra partition that should be assigned locally at this instance
					int moreForThisInst = pCnt - (sessionCnt * partForAll);

					// Minimum extra partitions must be assigned to this instance
					int extraNow = (sessionCnt == 0 || pCnt == 0) ? 0
							: Math.min(sessionCnt, moreForThisInst < 0 ? 0 : moreForThisInst);
					extraPerInst[instNow - 1] = Math.min(partForSome, extraNow);

					// System.out.println("TxEQAssignor: Extra for Inst " + instNow + "= [" +
					// extraPerInst[instNow-1] + "," + extraPerInstMax[instNow-1]+"]");
					partForSome = partForSome - extraPerInst[instNow - 1];
					extraPerInstMax[instNow - 1] = extraPerInstMax[instNow - 1] - extraPerInst[instNow - 1];

					if (partForSome <= 0)
						break;
				}

				// Rest of the <partForSome> partitions can be distributed among any sessions as
				// they will be assigned remotely
				for (int i = 0; (i < maxInst && partForSome > 0); i++) {
					if (extraPerInstMax[i] > 0) {
						int extra = Math.min(extraPerInstMax[i], partForSome);
						extraPerInst[i] += extra;
						partForSome = partForSome - extraPerInst[i];
						extraPerInstMax[i] -= extra;
					}
				}
				/*
				 * System.out.println("TxEQAssignor: Actual Extra per Partition "); for(int i =0
				 * ; i<maxInst ; i++ ) { System.out.println("TxEQAssignor: Inst: " + (i+1) +
				 * " Min Extra " + extraPerInst[i] +" Additional Extra " + extraPerInstMax[i] );
				 * } // Assign one additional partition to a session System.out.
				 * println("TxEQAssignor: Before actual assignment, Final pending partition count per session:"
				 * );
				 */
				for (SessionData tMem : memberList) {
					int instNow = tMem.getInstanceId();
					// Check if Extra Partition to be assigned for session connected to instNow
					if (extraPerInst[instNow - 1] > 0) {
						tMem.pendingCnt++;
						extraPerInst[instNow - 1]--;
					}
					// System.out.println("TxEQAssignor: Session " + tMem.name + " To be assigned "
					// + tMem.pendingCnt);
				}
			}

			// Logic to actual assignment of partition to session based on pendingCnt
			Iterator<SessionData> assigner = memberList.iterator();
			// System.out.println("TxEQAssignor: TxEQ Assign 3:Local Assignment");
			// Phase 1: Sticky-Local Assignment
			while (assigner.hasNext()) {
				SessionData tMem = assigner.next();
				// System.out.println("TxEQAssignor: Member " + tMem.name + " Pending partitions
				// " + tMem.pendingCnt);
				if (tMem.pendingCnt <= 0)
					continue;

				ArrayList<Integer> localPartitionList = instPListMap.get(tMem.getInstanceId());
				List<PartitionData> previousPartitions = tMem.getPreviousPartitions();
				if (localPartitionList != null && previousPartitions != null && previousPartitions.size() > 0) {
					for (PartitionData partNow : previousPartitions) {
						if (tMem.pendingCnt <= 0)
							break;

						// Confirm that partition belongs to the local instance
						// Check all partitions because due to instance shutdown a remote partition may
						// have moved to local instance
						int pIndex = localPartitionList.indexOf(partNow.getTopicPartition().partition());
						if (pIndex != -1) {
							tMem.pendingCnt--;
							tMem.addAssignedPartitions(partNow);
							localPartitionList.remove(pIndex);
							// System.out.println("Partitoin " +partNow.getTopicPartition().partition() + "
							// assigned to " + tMem.name +" Pending " + tMem.pendingCnt);
						}
					}
					previousPartitions.removeAll(tMem.getAssignedPartitions());
				}
			}
			// System.out.println("TxEQAssignor 4:Local New Assignment");
			assigner = memberList.iterator();
			// Phase 2: New Local Assignment
			while (assigner.hasNext()) {
				SessionData tMem = assigner.next();
				// System.out.println("TxEQAssignor: Member " + tMem.name + " Pending partitions
				// " + tMem.pendingCnt);
				if (tMem.pendingCnt <= 0)
					continue;

				ArrayList<Integer> localPartitionList = instPListMap.get(tMem.getInstanceId());

				if (localPartitionList == null || localPartitionList.size() == 0)
					break;

				ArrayList<Integer> assignedNow = new ArrayList<Integer>();

				for (Integer pNow : localPartitionList) {
					if (tMem.pendingCnt <= 0)
						break;

					PartitionData teqP = new PartitionData(topicNow, tMem.getQueueId(), pNow, tMem.getSubscriberName(),
							tMem.getSubscriberId(), tMem.getInstanceId(), true);
					tMem.addAssignedPartitions(teqP);
					tMem.pendingCnt--;
					assignedNow.add(pNow);
					// System.out.println("TxEQAssignor: Partitoin " +pNow + " assigned to " +
					// tMem.name +" Pending " + tMem.pendingCnt);
				}
				localPartitionList.removeAll(assignedNow);
				assignedNow.clear();
			}

			assigner = memberList.iterator();
			// Phase 3: Sticky Remote Assignment
			// System.out.println("TxEQAssignor: TxEQ Assign 5:Sticky Remote Assignment");
			while (assigner.hasNext()) {
				SessionData tMem = assigner.next();
				// System.out.println("TxEQAssignor: Member " + tMem.name + " Pending partitions
				// " + tMem.pendingCnt);

				if (tMem.pendingCnt <= 0)
					continue;

				List<PartitionData> previousPartitions = tMem.getPreviousPartitions();
				if (previousPartitions != null && previousPartitions.size() > 0) {
					ArrayList<PartitionData> removeList = new ArrayList<PartitionData>();
					for (PartitionData pNow : previousPartitions) {
						if (tMem.pendingCnt <= 0)
							break;
						int pInstLookup = pNow.getOwnerInstanceId();
						ArrayList<Integer> remotePartitionList = instPListMap.get(pInstLookup);

						int pIndex;
						// Search if Partition is still available in the expected partition list
						if (remotePartitionList == null)
							pIndex = -1;
						else
							pIndex = remotePartitionList.indexOf(pNow.getTopicPartition().partition());
						if (pIndex == -1) {
							// lookup in other instances if partition is shifted due to instance shutdown
							for (Integer instnow : instPListMap.keySet()) {
								if (instnow.intValue() == pInstLookup)
									continue;

								remotePartitionList = instPListMap.get(instnow.intValue());
								pIndex = remotePartitionList.indexOf(pNow.getTopicPartition().partition());
								if (pIndex != -1) {
									pInstLookup = instnow.intValue();
									break;
								}
							}
						}
						// Partition is available in any of the instance partition list
						if (pIndex != -1) {
							pNow.setOwnerInstanceId(pInstLookup);
							pNow.setLocal(false);
							tMem.pendingCnt--;
							tMem.addAssignedPartitions(pNow);
							remotePartitionList.remove(pIndex);
							removeList.add(pNow);
						}
					}
					previousPartitions.removeAll(removeList);
				}
			}

			assigner = memberList.iterator();
			// Phase 4: New Remote Assignment
			// System.out.println("TxEQAssignor: TxEQ Assign 6:Remote New Assignment");
			while (assigner.hasNext()) {
				SessionData tMem = assigner.next();
				// System.out.println("TxEQAssignor: Member " + tMem.name + " Pending partitions
				// " + tMem.pendingCnt);
				if (tMem.pendingCnt <= 0) {
					continue;
				}
				ArrayList<Integer> remotePartitionList = null;

				for (Integer instnow : instPListMap.keySet()) {
					if (tMem.pendingCnt <= 0)
						break;
					int instLookup = instnow.intValue();
					// Local Instance list is already parsed
					if (instLookup == tMem.getInstanceId())
						continue;

					ArrayList<Integer> removePList = new ArrayList<Integer>();
					remotePartitionList = instPListMap.get(instLookup);
					for (Integer pNow : remotePartitionList) {
						if (tMem.pendingCnt <= 0)
							break;

						PartitionData teqP = new PartitionData(topicNow, tMem.getQueueId(), pNow.intValue(),
								tMem.getSubscriberName(), tMem.getSubscriberId(), instLookup, false);
						tMem.addAssignedPartitions(teqP);
						tMem.pendingCnt--;
						removePList.add(pNow);
					}
					remotePartitionList.removeAll(removePList);
					removePList.clear();
				}
			}

			assigner = memberList.iterator();
			// Create assignments now
			// System.out.println("TxEQAssignor: Final Partition Map ");
			while (assigner.hasNext()) {
				SessionData tMem = assigner.next();
				// System.out.println("TxEQAssignor: Session: " + tMem.name );
				List<PartitionData> previousPartitions = tMem.getPreviousPartitions();
				List<TopicPartition> assignedToMe = assignment.get(tMem.name);
				List<PartitionData> assignedPartitionList = tMem.getAssignedPartitions();
				// If no partitions is assigned, set -1 as assigned partition
				if (assignedPartitionList == null || assignedPartitionList.size() == 0) {
					PartitionData teqP = new PartitionData(topicNow, tMem.getQueueId(), -1, tMem.getSubscriberName(),
							tMem.getSubscriberId(), tMem.getInstanceId(), true);
					assignedPartitionList.add(teqP);
					assignedToMe.add(teqP.getTopicPartition());
					// System.out.println("TxEQAssignor: " + teqP.getTopicPartition().partition());
					previousPartitions.clear();
					continue;
				}
				for (PartitionData teqPNow : assignedPartitionList) {
					assignedToMe.add(teqPNow.getTopicPartition());
					// System.out.println("TxEQAssignor: " +
					// teqPNow.getTopicPartition().partition());
				}
				previousPartitions.clear();
				previousPartitions.addAll(tMem.getAssignedPartitions());
			}
		}
		return assignment;
	}

	/*	if(clusterNow == null)
	{
		//clusterNow = updateMetaData();
	}
	List<PartitionInfo> partByTopic = clusterNow.availablePartitionsForTopic(topicNow);
	Map<Integer, ArrayList<PartitionInfo>> instPListMap = new HashMap<Integer,ArrayList<PartitionInfo>>();

	//Prepare Map of Partitions per Instance
	for(PartitionInfo pInfoNow : partByTopic)
	{
		Node leaderNode = pInfoNow.leader();
		Integer pForThisNode = instPCntMap.get(leaderNode.id());
		if(pForThisNode == null) {
			instPCntMap.put(leaderNode.id(),0);
			pForThisNode = 0;
			ArrayList<PartitionInfo> pInstInfoListNow = new ArrayList<PartitionInfo>();
			instPListMap.put(leaderNode.id(), pInstInfoListNow);
		}
		instPCntMap.put(leaderNode.id(), pForThisNode+1);
		instPListMap.get(leaderNode.id()).add(pInfoNow);
	}
	 */

	public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
			Map<String, Subscription> subscriptions) {
		SortedSet<String> topics = new TreeSet<>();
		for (Subscription subscription : subscriptions.values())
			topics.addAll(subscription.topics());

		List<TopicPartition> allPartitions = new ArrayList<>();
		for (String topic : topics) {
			Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
			if (numPartitionsForTopic != null)
				allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
		}
		return allPartitions;
	}

	@Override
	public String name() {
		return "TxEQAssignor";
	}

}
