package org.hps;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.kafka.clients.admin.*;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import static java.time.Duration.*;

public class Scaler {

    static RLS rls;
    static double[][] Xx;
    static double [] pred;
    static RealMatrix X;
    static int iteration = 0;

    Scaler(){
    }

    public static final String CONSUMER_GROUP = "testgroup3";
    public static  int numberOfPartitions;
    static boolean  scaled = false;
    public static  AdminClient admin = null;

    private static final Logger log = LogManager.getLogger(Scaler.class);

    public static Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();

    public static Map<TopicPartition, Long> previousPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> currentPartitionToLastOffset = new HashMap<>();

    public static Map<TopicPartition, Long> partitionToLag = new HashMap<>();

    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();

    static boolean  firstIteration = true;
    static Long sleep;
    static Long waitingTime;
    static String topic;
    static String cluster;
    static String consumerGroup;
    static Long coolDownPeriod = 60000L;  // 1 minuite and half
    private static Instant start = null;
    static long elapsedTime;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //TODO Externalize topic, cluster name, and all configurations

        sleep = Long.valueOf(System.getenv("SLEEP"));
        waitingTime = Long.valueOf(System.getenv("WAITING_TIME"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        //consumerGroup = System.getenv("CONSUMER_GROUP");

        log.info("sleep is {}", sleep);
        log.info("waiting time  is {}", waitingTime);
        log.info("topic is  {}", waitingTime);
        log.info(" consumerGroup {}", waitingTime);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
        admin   = AdminClient.create(props);
        /////////////////////////////////////////////////////////////
        rls = new RLS(4, 0.95);
        iteration = 0;
        Xx = new double[1][4];
        //pred = new double[50];

        for (int j = 0; j < 4; j++)
            Xx[0][j] = 0;
        ///////////////////////////////////////////////////////////////////////////////////////
        while (true) {
            log.info("====================================Start Iteration==============================================");
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                            .partitionsToOffsetAndMetadata().get();

            numberOfPartitions = offsets.size();

            Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();

            //initialize consumer to lag to 0
            for (TopicPartition tp : offsets.keySet()) {
                requestLatestOffsets.put(tp, OffsetSpec.latest());
                partitionToLag.put(tp, 0L);
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    admin.listOffsets(requestLatestOffsets).all().get();
            //////////////////////////////////////////////////////////////////////
            // Partition Statistics
            /////////////////////////////////////////////////////////////////////
            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
                long committedOffset = e.getValue().offset();
                long latestOffset = latestOffsets.get(e.getKey()).offset();
                long lag = latestOffset - committedOffset;

                if (!firstIteration) {
                    previousPartitionToCommittedOffset.put(e.getKey(), currentPartitionToCommittedOffset.get(e.getKey()));
                    previousPartitionToLastOffset.put(e.getKey(), currentPartitionToLastOffset.get(e.getKey()));
                }
                currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                currentPartitionToLastOffset.put(e.getKey(), latestOffset);
                partitionToLag.put(e.getKey(), lag);
            }


            //////////////////////////////////////////////////////////////////////
            // consumer group statistics
            /////////////////////////////////////////////////////////////////////
            log.info(" consumer group descriptions: ");

            /* lag per consumer */
            Long lag = 0L;
            //get information on consumer groups, their partitions and their members
            DescribeConsumerGroupsResult describeConsumerGroupsResult =
                    admin.describeConsumerGroups(Collections.singletonList(Scaler.CONSUMER_GROUP));
            KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                    describeConsumerGroupsResult.all();
            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

            //compute lag per consumer

            log.info(" =========================================");
            log.info(" Member consumer to partitions assignments");
            log.info(" ========================================");

            Set<MemberDescription> previousConsumers = new HashSet<MemberDescription>(consumerToLag.keySet());
            for(MemberDescription md : previousConsumers) {
                    if(consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().contains(md)){
                        continue;
                    }

                consumerToLag.remove(md);
                maxConsumptionRatePerConsumer.remove(md);
            }
            //////////////////////////////////

            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                MemberAssignment memberAssignment = memberDescription.assignment();
                log.info("Member {} has the following assignments", memberDescription.consumerId());
                for (TopicPartition tp : memberAssignment.topicPartitions()) {
                    log.info("\tpartition {}", tp.toString());
                    lag += partitionToLag.get(tp);
                }

                consumerToLag.put(memberDescription, lag);
                lag = 0L;
            }

            log.info(" =======================================");
            log.info(" Lag per consumer");
            log.info(" =======================================");


            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                log.info("The total lag  for partitions owned by  member {} is  {}", memberDescription.consumerId(),
                        consumerToLag.get(memberDescription));
            }

            log.info("================================================");


            if(!firstIteration ) {
                if(scaled) {
                    Instant now = Instant.now();
                    elapsedTime = Duration.between(start, now).toMinutes();
                    log.info("start {}, now {}", start , now);
                    log.info("Elapsed minutes since last scale {}", Duration.between(start, now).getSeconds());
                    if(elapsedTime >= 1L) {
                        scaled = false;
                        scaleDecision2(consumerGroupDescriptionMap);
                    }
                }
            }

            if(!scaled && !firstIteration) {
                log.info("calling scale decision neither scaled nor in first iteration");
                scaleDecision2(consumerGroupDescriptionMap);
            }



        if (firstIteration) {
            log.info("This is First iteration no scaling decisions");
        }

            firstIteration = false;
            log.info("Sleeping for sleep {}", sleep);
            Thread.sleep(sleep);
            if (scaled) {
               log.info("looks like we are in the cool down period");
            } else {
                log.info("we did not scale though outside of the cool down period");
            }

            log.info("====================================End Iteration==============================================");
        }
    }



    static void scaleDecision( Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {

        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();

        log.info("Logging current consumer rates:");
        log.info("=================================:");
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            log.info("current maximum consumption rate for consumer {} is {}, ", memberDescription,
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
        }
        log.info("=================================:");
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            long totalpoff = 0;
            long totalcoff = 0;
            long totalepoff = 0;
            long totalecoff = 0;

            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }
            log.info("=================================:");

            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;

            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }

            log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    consumptionRatePerConsumer * 1000);
            log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    arrivalRatePerConsumer * 1000);

            if (consumptionRatePerConsumer > maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer * 1000,
                        maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }

            if (consumerToLag.get(memberDescription) > (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime))
             /*consumptionRatePerConsumer * waitingTime)*/ {
                log.info("The magic formula for consumer {} does NOT hold I am going to scale by one for now : lag {}, " +
                                "consumptionRatePerConsumer * waitingTime {} ", memberDescription.consumerId(),
                        consumerToLag.get(memberDescription), /*consumptionRatePerConsumer * waitingTime*/
                        (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime));

                if (size < numberOfPartitions) {
                    log.info("Consumers are less than nb partition we can scale");
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                        scaled = true;
                        start = Instant.now();
                        //TODO
                        // is that needed, ? a cool down period?
                        //sleep = 2 * sleep;
                        break;
                    }
                } else {
                    log.info("Consumers are equal to nb partitions we can not scale anymore");
                }

            }
            log.info("Next consumer scale up");
            log.info("=================================:");
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////

        HashMap<MemberDescription, Long> temp = sortConsumerGroupDescriptionMap();
        // TODO ConsumerGroupDescriptionMap by descending lag with
        // TODO with respect to consumertolag
        ///////////////////////////////////////////////////////////////
        log.info("print sorted consumers by lag");
        log.info("=================================:");
        for (Map.Entry<MemberDescription, Long> memberDescription : temp.entrySet()) {
            log.info("sorted consumer id {} has the following lag {}", memberDescription.getKey().consumerId(),
                    memberDescription.getValue());
        }
        log.info("=================================:");




        if (!scaled) {
            for (MemberDescription memberDescription : temp.keySet()) {
                log.info("Begin Iteration scale down");
                log.info("=================================:");
                long totalpoff = 0;
                long totalcoff = 0;
                long totalepoff = 0;
                long totalecoff = 0;

                for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                    totalpoff += previousPartitionToCommittedOffset.get(tp);
                    totalcoff += currentPartitionToCommittedOffset.get(tp);
                    totalepoff += previousPartitionToLastOffset.get(tp);
                    totalecoff += currentPartitionToLastOffset.get(tp);
                }

                float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
                float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;


                log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                        consumptionRatePerConsumer * 1000);
                log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                        arrivalRatePerConsumer * 1000);


                /////////////////////////////////////////////////////////////////////////////////////////////

                if (consumptionRatePerConsumer >  maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                    log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer * 1000,
                            maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
                    maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
                }



                if(arrivalRatePerConsumer >= maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) /*consumptionRatePerConsumer)*/ {
                    log.info("I am not going to downscale consumer {} since  arrivalRatePerConsumer >= " +
                            "consumptionRatePerConsumer", memberDescription.consumerId());
                    continue;
                }


                if (consumerToLag.get(memberDescription) < (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)))
                    /*consumptionRatePerConsumer * waitingTime)*/ {
                      /*  log.info("The magic formula for consumer {} does hold I am going to down scale by one for now as trial",
                                memberDescription.consumerId());*/

                    log.info("The magic formula for consumer {} does hold I am going to down scale by one for now as trial, " +
                                    "lag {}, consumptionRatePerConsumer * waitingTime {} ", memberDescription.consumerId(),
                            consumerToLag.get(memberDescription), (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime)
                            /*consumptionRatePerConsumer * waitingTime*/);

                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                        if (replicas > 1) {
                            k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                            // firstIteration = true;
                            scaled = true;
                            start = Instant.now();

                            // recheck is this is needed....
                            //sleep = 2 * sleep;
                            break;
                        } else {
                            log.info("Not going to scale since replicas already one");
                        }
                    }
                }

                log.info("End Iteration scale down");
                log.info("=================================:");
            }
        }
    }














    /////////////////////////////////////////////////////////////////////////////////////////

    // when downscaling delete the consumer with minimum lag
    static MemberDescription getConsumeWithLowestLag( Map<MemberDescription, Long> consumerToLag) {
        Map.Entry<MemberDescription, Long> min = null;
        for (Map.Entry<MemberDescription, Long> entry : consumerToLag.entrySet()) {
            if (min == null || min.getValue() > entry.getValue()) {
                min = entry;
            }
        }
        return min.getKey();
    }



    // when downscaling delete the consumer with minimum lag
    static  MemberDescription getConsumeWithLargestLag(Map<MemberDescription, Long> consumerToLag) {
        Map.Entry<MemberDescription, Long> max = null;
        for (Map.Entry<MemberDescription, Long> entry : consumerToLag.entrySet()) {
            if (max  == null || max.getValue() < entry.getValue()) {
                max = entry;
            }
        }
        return max.getKey();
    }


    static HashMap<MemberDescription, Long> sortConsumerGroupDescriptionMap() {

        // Create a list from elements of HashMap
        List<Map.Entry<MemberDescription, Long>> list =
                new LinkedList<Map.Entry<MemberDescription, Long> >(consumerToLag.entrySet());

        // Sort the list
        Collections.sort(list, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

        // put data from sorted list to hashmap
        HashMap<MemberDescription, Long> temp = new LinkedHashMap<>();
        for (Map.Entry<MemberDescription, Long> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
}

    static void scaleDecision2(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {


        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        float totallag = 0;

        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();

        log.info("Logging current consumer rates:");
        log.info("=================================:");


        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            log.info("current maximum consumption rate for consumer {} is {}, ", memberDescription,
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
        }

        log.info("=================================:");


        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            long totalpoff = 0;
            long totalcoff = 0;
            long totalepoff = 0;
            long totalecoff = 0;

            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }
            log.info("=================================:");

            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;

            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }


            log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    consumptionRatePerConsumer * 1000);
            log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    arrivalRatePerConsumer * 1000);

            if (consumptionRatePerConsumer > maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer * 1000,
                        maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }


            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;

            log.info("totalArrivalRate {}, totalconsumptionRate{}", totalConsumptionRate * 1000, totalArrivalRate * 1000);



        /* if (consumerToLag.get(memberDescription) > (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime))
         *//*consumptionRatePerConsumer * waitingTime)*//* {
                log.info("The magic formula for consumer {} does NOT hold I am going to scale by one for now : lag {}, " +
                                "consumptionRatePerConsumer * waitingTime {} ", memberDescription.consumerId(),
                        consumerToLag.get(memberDescription), *//*consumptionRatePerConsumer * waitingTime*//*
                        (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime));

                if (size < numberOfPartitions) {
                    log.info("Consumers are less than nb partition we can scale");
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                        scaled = true;
                        start = Instant.now();
                        //TODO
                        // is that needed, ? a cool down period?
                        //sleep = 2 * sleep;
                        break;
                    }
                } else {
                    log.info("Consumers are equal to nb partitions we can not scale anymore");
                }

            }
            log.info("Next consumer scale up");
            log.info("=================================:");*/
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////




	/*	for (int j=1; j<4; j++)
			Xx[0][j]= Xx[0][j-1];*/

/*    if(iteration<4) {
        Xx[0][iteration] = Double.parseDouble(String.valueOf(totalArrivalRate));

        log.info("no prediction model is not filled yet");
    } else  {*/

        if (iteration < 4) {

            log.info("no prediction iteration less than 4");
            log.info("iteration = {}", iteration);
            log.info("total arrivals since the last monitoring interval {}", (float) (totalArrivalRate * sleep));

            for (int j = 0; j < 3; j++)
                Xx[0][j] = Xx[0][j + 1];

            double y1 = Double.parseDouble(String.valueOf((float) (totalArrivalRate * sleep)));
            Xx[0][3] = y1;

            X = new Array2DRowRealMatrix(Xx);

            log.info("X = {}", X);

        } else {

            log.info("before y");
            double y1 = Double.parseDouble(String.valueOf(totalArrivalRate * sleep));
            log.info("after y");

            log.info("total arrival  of authors since last interval {}", (float) (totalArrivalRate * sleep));


            for (int j = 0; j < 3; j++)
                Xx[0][j] = Xx[0][j + 1];
            Xx[0][3] = y1;


            X = new Array2DRowRealMatrix(Xx);

            log.info("X = {}", X);

            rls.add_obs(X.transpose(), y1);

            double prediction = (rls.getW().transpose().multiply(X.transpose())).getEntry(0, 0);

            log.info(" iteration {} current total arrival since last monitoring interval {}, my prediction for the next one {}", iteration,
                    (float) (totalArrivalRate * sleep), prediction);
        }
        iteration++;


  /*  if (totalArrivalRate > totalConsumptionRate) {

        if (size < numberOfPartitions) {
            log.info("Consumers are less than nb partition we can scale");
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                scaled = true;
                start = Instant.now();
                //TODO
                // is that needed, ? a cool down period?
                //sleep = 2 * sleep;
            }
        } else {
            log.info("Consumers are equal to nb partitions we can not scale anymore");
        }


    log.info("Next consumer scale up");
    log.info("=================================:");

}  else if (totalArrivalRate < totalConsumptionRate) {


        try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
            ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
            k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
            int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
            if (replicas > 1) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                // firstIteration = true;

                scaled = true;
                start = Instant.now();

                // recheck is this is needed....
                //sleep = 2 * sleep
            } else {
                log.info("Not going to scale since replicas already one");
            }
        }


    log.info("End Iteration scale down");
    log.info("=================================:");

    }
*/





        ///////////////////////////////////////////////////////////////////////////////////////////////////

       /* HashMap<MemberDescription, Long> temp = sortConsumerGroupDescriptionMap();
        // TODO ConsumerGroupDescriptionMap by descending lag with
        // TODO with respect to consumertolag
        ///////////////////////////////////////////////////////////////
        log.info("print sorted consumers by lag");
        log.info("=================================:");
        for (Map.Entry<MemberDescription, Long> memberDescription : temp.entrySet()) {
            log.info("sorted consumer id {} has the following lag {}", memberDescription.getKey().consumerId(),
                    memberDescription.getValue());
        }
        log.info("=================================:");




        if (!scaled) {
            for (MemberDescription memberDescription : temp.keySet()) {
                log.info("Begin Iteration scale down");
                log.info("=================================:");
                long totalpoff = 0;
                long totalcoff = 0;
                long totalepoff = 0;
                long totalecoff = 0;

                for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                    totalpoff += previousPartitionToCommittedOffset.get(tp);
                    totalcoff += currentPartitionToCommittedOffset.get(tp);
                    totalepoff += previousPartitionToLastOffset.get(tp);
                    totalecoff += currentPartitionToLastOffset.get(tp);
                }

                float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
                float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;


                log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                        consumptionRatePerConsumer * 1000);
                log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                        arrivalRatePerConsumer * 1000);


                /////////////////////////////////////////////////////////////////////////////////////////////

                if (consumptionRatePerConsumer >  maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                    log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer * 1000,
                            maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
                    maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
                }



                if(arrivalRatePerConsumer >= maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) *//*consumptionRatePerConsumer)*//* {
                    log.info("I am not going to downscale consumer {} since  arrivalRatePerConsumer >= " +
                            "consumptionRatePerConsumer", memberDescription.consumerId());
                    continue;
                }


                if (consumerToLag.get(memberDescription) < (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)))
                    *//*consumptionRatePerConsumer * waitingTime)*//* {
                      *//*  log.info("The magic formula for consumer {} does hold I am going to down scale by one for now as trial",
                                memberDescription.consumerId());*//*

                    log.info("The magic formula for consumer {} does hold I am going to down scale by one for now as trial, " +
                                    "lag {}, consumptionRatePerConsumer * waitingTime {} ", memberDescription.consumerId(),
                            consumerToLag.get(memberDescription), (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime)
                            *//*consumptionRatePerConsumer * waitingTime*//*);

                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                        if (replicas > 1) {
                            k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                            // firstIteration = true;
                            scaled = true;
                            start = Instant.now();

                            // recheck is this is needed....
                            //sleep = 2 * sleep;
                            break;
                        } else {
                            log.info("Not going to scale since replicas already one");
                        }
                    }
                }

                log.info("End Iteration scale down");
                log.info("=================================:");
            }
        }*/
    }
}














