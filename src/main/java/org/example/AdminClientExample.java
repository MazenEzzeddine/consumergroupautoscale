package org.example;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class AdminClientExample  {



    // this code is functional but need refactoring

    public static final String TOPIC_NAME = "testtopic2";
    public static final String CONSUMER_GROUP = "testgroup2";
    public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
    public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
    public static  int NUM_PARTITIONS;
    public static final short REP_FACTOR = 1;

    long previousComittedOffsets = 0;
    long currentComittedOffsets = 0;

    static boolean  scaled= false;


    public static  AdminClient admin = null;

    // public static  = null;

    static int scale = 2;


    private static final Logger log = LogManager.getLogger(AdminClientExample.class);



    private static Properties consumerGroupProps;
    private static Properties metadataConsumerProps;
    private static KafkaConsumer<byte[], byte[]> metadataConsumer;


   /* @Override
    public void configure(Map<String, ?> configs) {

        // Construct Properties from config map
        consumerGroupProps = new Properties();
        for (final Map.Entry<String, ?> prop : configs.entrySet()) {
            consumerGroupProps.put(prop.getKey(), prop.getValue());
        }

        // group.id must be defined
        final String groupId = "testgroup2";//consumerGroupProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);


        // Create a new consumer that can be used to get lag metadata for the consumer group
        metadataConsumerProps = new Properties();
        metadataConsumerProps.putAll(consumerGroupProps);
        metadataConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        final String clientId = groupId + ".metadada";
        metadataConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        log.info(
                "Configured LagBasedPartitionAssignor with values:\n"
                        + "\tgroup.id = {}\n"
                        + "\tclient.id = {}\n",
                groupId,
                clientId
        );

    }*/





    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // initialize admin client

        // this is feature 2


        ///////////////////testing a metadataconsumer to enforcerebelabce////////////////////
        Properties props2 = new Properties();
        props2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props2.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");

        props2.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());
        props2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        log.info("creating meta data consumer");
        if (metadataConsumer == null) {
            metadataConsumer = new KafkaConsumer<>(props2);
            log.info("created meta data consumer");

        }



        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String topicg = System.getenv("TOPIC");
        Long sleep = Long.valueOf(System.getenv("SLEEP"));
        Long waitingTime = Long.valueOf(System.getenv("WAITING_TIME"));
        boolean firstIteration = true;


        log.info("sleep is {}", sleep);

        log.info("waiting time  is {}", waitingTime);



        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
         admin   = AdminClient.create(props);
        TopicDescription topicDescription;

        long totalOffset =0;
        Long currentCommittedOffsets=0L;

        Long previousCommittedOffsets=0L;

        Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
        Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();


        ///////////////////////////////////////////////////////////////////////////////////////





        while (true) {



            ///////////////////testing number of replicas////////////////////

            try (final KubernetesClient k8ss = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8ss.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8ss.apps().deployments().inNamespace("default").withName("cons1pss").get().getSpec().getReplicas();

                log.info("current number of consumer replicas is {}", replicas);
            }
                // firstIteration = true;





            //////////////////////////////////////////////////////////



            Map<TopicPartition, Long> partitionToLag = new HashMap<>();


            // list topics
            //comment
            ListTopicsResult topics = admin.listTopics();
            topics.names().get().forEach(name -> log.info("topic name {}", name));


            log.info("Listing consumer groups, if any exist:");
            admin.listConsumerGroups().valid().get().forEach(name -> log.info("topic name {}\n", name));



            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                            .partitionsToOffsetAndMetadata().get();
            NUM_PARTITIONS = offsets.size();


            Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
            Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();
            Map<TopicPartition, OffsetSpec> requestOlderOffsets = new HashMap<>();
            DateTime resetTo = new DateTime().minusHours(2);
            // For all topics and partitions that have offsets committed by the group, get their latest offsets, earliest offsets
            // and the offset for 2h ago. Note that I'm populating the request for 2h old offsets, but not using them.
            // You can swap the use of "Earliest" in the `alterConsumerGroupOffset` example with the offsets from 2h ago
            for (TopicPartition tp : offsets.keySet()) {
                requestLatestOffsets.put(tp, OffsetSpec.latest());
                requestEarliestOffsets.put(tp, OffsetSpec.earliest());
                requestOlderOffsets.put(tp, OffsetSpec.forTimestamp(resetTo.getMillis()));
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    admin.listOffsets(requestLatestOffsets).all().get();

            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
                String topic = e.getKey().topic();
                int partition = e.getKey().partition();
                long committedOffset = e.getValue().offset();
                long latestOffset = latestOffsets.get(e.getKey()).offset();
                long lag = latestOffset - committedOffset;
              //previousPartitionToCommittedOffset.put(e.getKey(), );

                if (firstIteration) {

                    currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                } else {

                    previousPartitionToCommittedOffset.put(e.getKey(),  currentPartitionToCommittedOffset.get(e.getKey()));

                    currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                }
              //  currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                partitionToLag.put(e.getKey(), lag);

                log.info("Consumer group " + CONSUMER_GROUP
                        + " has committed offset " + committedOffset
                        + " to topic " + topic + " partition " + partition
                        + ". The latest offset in the partition is "
                        + latestOffset + " so consumer group is "
                        + (lag) + " records behind");

                totalOffset +=  lag;
            }



            if (! firstIteration) {
                for (Map.Entry<TopicPartition, Long> entry : previousPartitionToCommittedOffset.entrySet()) {
                    log.info("for partition {} previousCommittedOffsets = {}", entry.getKey(),
                            previousPartitionToCommittedOffset.get(entry.getKey()));


                    log.info(" for partition {} currentCommittedOffsets = {}", entry.getKey(),
                            currentPartitionToCommittedOffset.get(entry.getKey()));


                }
            }


            //PartitionToLag

            //ConsumerTolag

            log.info("printing partition lags");


            for(Map.Entry<TopicPartition, Long> entry :  partitionToLag.entrySet()) {
                log.info("partition {} has the following lag {}", entry.getKey().partition() , entry.getValue());
            }



            log.info("Call to CG description ");

            Map<MemberDescription, Long> consumerToLag = new HashMap<>();
            Set<TopicPartition> topicPartitions = new HashSet<>();

            Long lag =0L;
            DescribeConsumerGroupsResult describeConsumerGroupsResult =
                    admin.describeConsumerGroups(Collections.singletonList(AdminClientExample.CONSUMER_GROUP));
            KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                    describeConsumerGroupsResult.all();
            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
           Map<String, Set<TopicPartition>> memberToTopicPartitionMap = new HashMap<>();
            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(AdminClientExample.CONSUMER_GROUP).members()) {
                MemberAssignment memberAssignment = memberDescription.assignment();
                log.info("member {} has the following assignments", memberDescription.consumerId());
                topicPartitions = memberAssignment.topicPartitions();
               memberToTopicPartitionMap.put(memberDescription.consumerId(), topicPartitions);
                for (TopicPartition tp : memberAssignment.topicPartitions()) {
                    log.info("\tpartition {}", tp.toString());
                     lag += partitionToLag.get(tp);

                }

               // consumerToLag.put(memberDescription, lag);

                consumerToLag.putIfAbsent(memberDescription, lag);
                lag = 0L;

            }


            //print lag per consumer


            log.info("We are going to print lag per consumer");

          for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(AdminClientExample.CONSUMER_GROUP).members()) {
             log.info("the total offsets for partitions owend by  member {} is  {}", memberDescription.consumerId(),
                      consumerToLag.get(memberDescription));



          }


            log.info("Enforcing Rebalance trial");
            metadataConsumer.enforceRebalance();




            ///////////////////////////////////////////////////////////////////////////
           // Map<String, Map<TopicPartition, Long>> memberToTopicPartitioAndLagnMap = new HashMap<>();






            int size =  consumerGroupDescriptionMap.get(AdminClientExample.CONSUMER_GROUP).members().size();


            if (! firstIteration) {
            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(AdminClientExample.CONSUMER_GROUP).members()) {

                long totalpoff =0;
                long totalcoff=0;


                      for(TopicPartition tp :  memberDescription.assignment().topicPartitions()) {
                          totalpoff += previousPartitionToCommittedOffset.get(tp);
                          totalcoff += currentPartitionToCommittedOffset.get(tp);
                      }

                      long ratePerConsumer = (totalcoff - totalpoff)/ 30;

                      log.info("the consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                      ratePerConsumer);


               if (consumerToLag.get(memberDescription) < (ratePerConsumer * waitingTime)){
                   log.info("the magic formula for consumer {} does not hols I am going to scale by one",  memberDescription.consumerId());

                   if (size < NUM_PARTITIONS) {
                       log.info("consumers are less than nb partition we can scale");


                       try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                           ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                           k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                           k8s.apps().deployments().inNamespace("default").withName("cons1pss").scale(size + 1);
                           // firstIteration = true;

                           scaled = true;
                           break;
                       }
                   } else {

                       log.info("consumers are equal  to nb partition we can not scale");
                       log.info("Enforcing Rebalance trial");
                       metadataConsumer.enforceRebalance();

                   }

               } else {
                   log.info("the magic formula for consumer {} does  hold need NOT to scale",  memberDescription.consumerId());

               }

            }
            }









            Thread.sleep(1000 * 30);

            totalOffset = 0;

            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(AdminClientExample.CONSUMER_GROUP).members()) {
               // log.info("the total offsets for partitions owend by  member {} is  {}", memberDescription.consumerId(),
                        consumerToLag.put(memberDescription, 0L);
            }

            firstIteration = false;


            if (scaled) {
                firstIteration = true;
                scaled = false;
            }


        }

    }





    public static  Map<String, Set<TopicPartition>> getConsumerGroupMemberInfo(ConsumerGroupDescription consumerGroupDescription) {
        Map<String, Set<TopicPartition>> memberToTopicPartitionMap = new HashMap<>();
        for (MemberDescription memberDescription : consumerGroupDescription.members()) {
            MemberAssignment memberAssignment = memberDescription.assignment();
            Set<TopicPartition> topicPartitions = memberAssignment.topicPartitions();
            memberToTopicPartitionMap.put(memberDescription.consumerId(), topicPartitions);
        }
        return memberToTopicPartitionMap;
    }



   static  Map<TopicPartition, Long> getConsumerGroupOffsetInfo(String consumerGroupID)
            throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
                admin.listConsumerGroupOffsets(consumerGroupID);
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> futureOfConsumerGroupOffsetResult =
                listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata();
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsetInfo = futureOfConsumerGroupOffsetResult.get();
        return consumerGroupOffsetInfo.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().offset()));
    }
}














