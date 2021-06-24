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

public class Scaler {
    static RLS rls;
    static double[][] Xx;
    static double[] pred;
    static RealMatrix X;
    static int iteration = 0;

    Scaler() {
    }

    public static final String CONSUMER_GROUP = "testgroup3";
    public static int numberOfPartitions;
    static boolean scaled = false;
    public static AdminClient admin = null;
    private static final Logger log = LogManager.getLogger(Scaler.class);
    public static Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> currentPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> partitionToLag = new HashMap<>();
    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();
    public static Map<TopicPartition, Long> PartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> PartitionToCommittedOffset = new HashMap<>();


    static boolean firstIteration = true;
    static Long sleep;
    static Long waitingTime;
    static String topic;
    static String cluster;
    static Long poll;
    static Long SEC;
    static String choice;

    static Float uth;
    static Float dth;



    //static Long sla;


    private static Instant start = null;
    static long elapsedTime;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //TODO Externalize topic, cluster name, and all configurations
        //TODO externalize autoscale decision logic Arriva or lag

        sleep = Long.valueOf(System.getenv("SLEEP"));
        waitingTime = Long.valueOf(System.getenv("WAITING_TIME"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        SEC = Long.valueOf(System.getenv("SEC"));
        choice = System.getenv("CHOICE");

        uth= Float.parseFloat(System.getenv("uth"));
        dth= Float.parseFloat(System.getenv("dth"));



        //sla = Long.valueOf(System.getenv("SLA"));


        //consumerGroup = System.getenv("CONSUMER_GROUP");

        log.info("sleep is {}", sleep);
        log.info("waiting time  is {}", waitingTime);
        log.info("topic is  {}", topic);

        log.info("poll is  {}", poll);
        log.info("SEC is  {}", SEC);

        log.info("uth is  {}", uth);
        log.info("dth is {}", dth);



        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
        admin = AdminClient.create(props);
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

                /*PartitionToCommittedOffset.put(e.getKey(), committedOffset);
                PartitionToLastOffset.put(e.getKey(), latestOffset);
                partitionToLag.put(e.getKey(), lag);*/
            }


            //////////////////////////////////////////////////////////////////////
            // consumer group statistics
            /////////////////////////////////////////////////////////////////////
//            log.info(" consumer group descriptions: ");

            /* lag per consumer */
            Long lag = 0L;
            //get information on consumer groups, their partitions and their members
            DescribeConsumerGroupsResult describeConsumerGroupsResult =
                    admin.describeConsumerGroups(Collections.singletonList(Scaler.CONSUMER_GROUP));
            KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                    describeConsumerGroupsResult.all();
            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

            //compute lag per consumer

     /*       log.info(" =========================================");
            log.info(" Member consumer to partitions assignments");
            log.info(" ========================================");*/


            // if a particular consumer is removed as a result of scaling decision remove
            Set<MemberDescription> previousConsumers = new HashSet<MemberDescription>(consumerToLag.keySet());
            for (MemberDescription md : previousConsumers) {
                if (consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().contains(md)) {
                    continue;
                }

                consumerToLag.remove(md);
                maxConsumptionRatePerConsumer.remove(md);
            }
            //////////////////////////////////

            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                MemberAssignment memberAssignment = memberDescription.assignment();
//                log.info("Member {} has the following assignments", memberDescription.consumerId());
                for (TopicPartition tp : memberAssignment.topicPartitions()) {
//                    log.info("\tpartition {}", tp.toString());
                    lag += partitionToLag.get(tp);
                }

                consumerToLag.put(memberDescription, lag);
                lag = 0L;
            }

           /* log.info(" =======================================");
            log.info(" Lag per consumer");
            log.info(" =======================================");


            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                log.info("The total lag  for partitions owned by  member {} is  {}", memberDescription.consumerId(),
                        consumerToLag.get(memberDescription));
            }*/


           /* log.info("calling scale decision neither scaled nor in first iteration");
            scaleDecision2(consumerGroupDescriptionMap);

            log.info("================================================");*/


            if (!firstIteration) {
//                if (scaled) {
//                    Instant now = Instant.now();
//                    elapsedTime = Duration.between(start, now).getSeconds();
//                    log.info("start {}, now {}", start, now);
//                    if (elapsedTime >= 30) {
//                        scaled = false;
                log.info("Yes scale decision NOT first iteration and/or scaled in previous ");

                if(choice.equals("arr")) {
                    scaleDecision2(consumerGroupDescriptionMap);
                } else if (choice.equals("arrpred")) {
                    scaleDecision5(consumerGroupDescriptionMap);
                } else {
                    scaleDecision3(consumerGroupDescriptionMap);
                }

                //scaleDecision3(consumerGroupDescriptionMap);
                        //log.info("Scale decision Elapsed minutes since last scale {}", Duration.between(start, now).getSeconds());

                    } else {

                        log.info("No scale decision Looks first iteration and/or scaled in previous ");
                        firstIteration = false;
//                    }
//                    //scaleDecision2(consumerGroupDescriptionMap);
//                } else {
//                    scaleDecision2(consumerGroupDescriptionMap);
//                }
            }


            log.info("sleeping for  {} secs", sleep);


            log.info("====================================End Iteration==============================================");
            Thread.sleep(sleep);
            //admin.metrics()

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
        long totallag = 0;

        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();

        log.info("Currently we have this number of consumers {}", size);


//        log.info("Logging current consumer rates:");
//        log.info("=================================:");
//
//
//        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
//            log.info("current maximum consumption rate for consumer {} is {}, ", memberDescription,
//                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
//        }
//
//        log.info("=================================:");


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


/*
            float consumptionRatePerConsumer =   ((float) (totalcoff - totalpoff) / sleep) * 1000.0f;
            float arrivalRatePerConsumer = ((float) (totalcoff - totalpoff) / sleep) * 1000.0f;*/

            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;


            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }

            log.info("=================================:");


          /*  log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    consumptionRatePerConsumer *1000);
            log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    arrivalRatePerConsumer *1000);*/

            if (consumptionRatePerConsumer > maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
            /*    log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer *1000,
                        maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) *1000);*/
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }


            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);




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


        /////////////////////////////////////////////////prediction code
        /*if (iteration < 3) {

            log.info("No prediction iteration less than 4");
            log.info("Iteration = {}", iteration);
            log.info("Total arrivals since the last monitoring interval {}", (float) (totalArrivalRate * sleep));

            for (int j = 0; j < 3; j++)
                Xx[0][j] = Xx[0][j + 1];

            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate * sleep)));
            Xx[0][3] = y1;

            X = new Array2DRowRealMatrix(Xx);



            log.info("X = {}", X);

            log.info( " iteration {} Authorizations consumed  since the last monitoring interval {}", iteration,  (float) (totalConsumptionRate * sleep));
            //rls.add_obs(X.transpose(), y1);


        } else {

            log.info("Before y");
            double y1 = Double.parseDouble(String.valueOf(totalArrivalRate * sleep));
            log.info("After y");

            log.info("Total arrival  of authors since last interval {}", (float) (totalArrivalRate * sleep));


            for (int j = 0; j < 3; j++)
                Xx[0][j] = Xx[0][j + 1];
            Xx[0][3] = y1;


            X = new Array2DRowRealMatrix(Xx);

            log.info("X = {}", X);

           // rls.add_obs(X.transpose(), y1);

            double prediction = (rls.getW().transpose().multiply(X.transpose())).getEntry(0, 0);

            log.info(" Iteration {} current total arrival since last monitoring interval {}, my prediction for the next one {}", iteration,
                    (float) (totalArrivalRate * sleep), prediction);

            log.info( " iteration {}  Authorizations consumed  since the last monitoring interval {}", iteration , (float) (totalConsumptionRate * sleep));
            log.info( " iteration {}  lag  observed  since the last monitoring interval {}", iteration , totallag);


            rls.add_obs(X.transpose(), y1);

        }*/
        iteration++;

        //we need the maximim total consulmer aret
        // when there is a lag it is operating at maximum

      /*  log.info(" total current  arrivals per seconds  {} , total  current  consumers  per seonds  {}",
                totalArrivalRate*1000, totalConsumptionRate*1000);*/
//        log.info("  total current  arrivals for the last 15 seconds {} , total   current  consumeed messages for last 15 messages  {}",
//                totalArrivalRate * 15 *1000, totalConsumptionRate * 15 * 1000);

        //if (totalArrivalRate * 15 * 1000 > 15 * size ) {
        // consumer is configured with maximum of messages per second consumption

        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate*1000, totalConsumptionRate*1000, totallag);

        log.info("shall we up scale totalArrivalrate {}, max  consumption rate {}",
                totalArrivalRate *1000, /*size *poll **/(size *poll* uth)/(float)SEC);

        if ((totalArrivalRate *1000) > ((size *poll * uth)/(float)SEC)) /*totalConsumptionRate *1000*/ {
            if (size < numberOfPartitions) {
                log.info("Consumers are less than nb partition we can scale");
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                    scaled = true;
                    start = Instant.now();
                    firstIteration = true;
            /*    log.info("since  arrivals in the last 15 secs  {} is greater than   consumed messages in the last 15,  I up scaled  by one {}",
                        totalArrivalRate * 1000 *15, totalConsumptionRate * 1000 *15);*/
                    log.info("since  arrival rate   {} is greater than  maximum consumed messages rate (size*poll/SEC) *uth ,  I up scaled  by one {}",
                            totalArrivalRate * 1000, /*size *poll*/ /*totalConsumptionRate *1000*/(size *poll*uth)/(float)SEC);
                    //TODO
                    // is that needed, ? a cool down period?
                    //sleep = 2 * sleep;
                }
            } else {
                log.info("Consumers are equal to nb partitions we can not scale up anymore");
            }
        }  //else if (totalArrivalRate < totalConsumptionRate) {
        //add another condition for lah check
        //to be done Estimated total arrival rate for next monitoring interval time
        else if (/*(totalArrivalRate *1000 < size *poll)*/ /*totalConsumptionRate *1000*/
                (totalArrivalRate *1000)  < (((size-1) *poll * dth)/(float)SEC)) /*totalConsumptionRate *1000*/ {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    // firstIteration = true;

                    scaled = true;
                    firstIteration = true;
                    start = Instant.now();

                    log.info("since   arrival rate {} is lower than max   consumption rate  with size -1 times dth, I down scaled  by one {}",
                            totalArrivalRate * 1000, /*totalConsumptionRate *1000 */ /*size *poll*/
                            ((size-1) *poll *dth)/(float)SEC);

//                log.info("since  rate   arrivals {} is smaller  than current   rate  consummed I down scaled  by one {}",
//                        totalArrivalRate *1000 /*15*/, totalConsumptionRate*1000/*15*/);

                    // recheck is this is needed....
                    //sleep = 2 * sleep
                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }

        log.info("quitting scale decision");

        log.info("=================================:");



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



/////////////////////////////////////////////////////////////


    static void scaleDecision3(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {
        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        long totallag = 0;
        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();
        log.info("Currently we have this number of consumers {}", size);

//        log.info("Logging current consumer rates:");
//        log.info("=================================:");
//
//
//        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
//            log.info("current maximum consumption rate for consumer {} is {}, ", memberDescription,
//                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
//        }
//
//        log.info("=================================:");
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

/*
            float consumptionRatePerConsumer =   ((float) (totalcoff - totalpoff) / sleep) * 1000.0f;
            float arrivalRatePerConsumer = ((float) (totalcoff - totalpoff) / sleep) * 1000.0f;*/

            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;


            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }



          /*  log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    consumptionRatePerConsumer *1000);
            log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    arrivalRatePerConsumer *1000);*/

            if (consumptionRatePerConsumer >
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
            /*    log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer *1000,
                        maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) *1000);*/
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }

            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);

        }

        log.info("=================================:");






        //////////////////////////////////////////////////////////////////////////////////////////////////////


        ///////////////////////////////////////////////////prediction code
//        if (iteration < 3) {
//
//            log.info("no prediction iteration less than 4");
//            log.info("iteration = {}", iteration);
//            log.info("total arrivals since the last monitoring interval {}", (float) (totalArrivalRate * sleep));
//
//            for (int j = 0; j < 3; j++)
//                Xx[0][j] = Xx[0][j + 1];
//
//            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate * sleep)));
//            Xx[0][3] = y1;
//
//            X = new Array2DRowRealMatrix(Xx);
//
//
//
//            log.info("X = {}", X);
//
//            log.info( " iteration {} Authorizations consumed  since the last monitoring interval {}", iteration,  (float) (totalConsumptionRate * sleep));
//            //rls.add_obs(X.transpose(), y1);
//
//
//        } else {
//
//            log.info("before y");
//            double y1 = Double.parseDouble(String.valueOf(totalArrivalRate * sleep));
//            log.info("after y");
//
//            log.info("total arrival  of authors since last interval {}", (float) (totalArrivalRate * sleep));
//
//
//            for (int j = 0; j < 3; j++)
//                Xx[0][j] = Xx[0][j + 1];
//            Xx[0][3] = y1;
//
//
//            X = new Array2DRowRealMatrix(Xx);
//
//            log.info("X = {}", X);
//
//           // rls.add_obs(X.transpose(), y1);
//
//            double prediction = (rls.getW().transpose().multiply(X.transpose())).getEntry(0, 0);
//
//            log.info(" iteration {} current total arrival since last monitoring interval {}, my prediction for the next one {}", iteration,
//                    (float) (totalArrivalRate * sleep), prediction);
//
//            log.info( " iteration {}  Authorizations consumed  since the last monitoring interval {}", iteration , (float) (totalConsumptionRate * sleep));
//            log.info( " iteration {}  lag  observed  since the last monitoring interval {}", iteration , totallag);
//
//
//            rls.add_obs(X.transpose(), y1);
//
//        }
        iteration++;

        //we need the maximim total consulmer aret
        // when there is a lag it is operating at maximum

      /*  log.info(" total current  arrivals per seconds  {} , total  current  consumers  per seonds  {}",
                totalArrivalRate*1000, totalConsumptionRate*1000);*/
//        log.info("  total current  arrivals for the last 15 seconds {} , total   current  consumeed messages for last 15 messages  {}",
//                totalArrivalRate * 15 *1000, totalConsumptionRate * 15 * 1000);

        //if (totalArrivalRate * 15 * 1000 > 15 * size ) {
        // consumer is configured with maximum of messages per second consumption

        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate*1000, totalConsumptionRate*1000, totallag);

        log.info("shall we scale totalArrivalrate {}, current  consumption rate {}",
                totalArrivalRate *1000, /*size *poll **/totalConsumptionRate *1000);

       // if ((totalArrivalRate *1000) > (size *poll)) /*totalConsumptionRate *1000*/ {
        if ((totallag) > (size *poll * waitingTime * uth )) /*totalConsumptionRate *1000*/ {
            log.info("totallag  {}, size *poll * waitingTime * uth {}", totallag , size *poll * waitingTime *uth);
            double ratio = ((double)totallag) / ((double)(size *poll * waitingTime*uth));
            ratio = Math.ceil(ratio);
            log.info("ratio after ceil {}", ratio);
            //int needed = (int)ratio -size;
            if (size < numberOfPartitions) {
                log.info("Consumers are less than nb partition we can scale");
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 =
                            new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                   /* k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale((int)ratio);*/
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size+1);
                    scaled = true;
                    firstIteration = true;
                    start = Instant.now();
            /*    log.info("since  arrivals in the last 15 secs  {} is greater than   consumed messages in the last 15,  I up scaled  by one {}",
                        totalArrivalRate * 1000 *15, totalConsumptionRate * 1000 *15);*/
                   /* log.info("since  total lag   {} is violates the SLA   (size*poll * waiting time *uth ) {} ,  I up scaled  by ratio {}",
                            totallag  , *//*size *poll*//* size *poll * waitingTime *uth, ratio);*/
                    log.info("since  total lag   {} is violates the SLA   (size*poll * waiting time *uth ) {} ,  I up scaled  by one",
                            totallag  , /*size *poll*/ size *poll * waitingTime *dth);
                    //TODO
                    // is that needed, ? a cool down period?
                    //sleep = 2 * sleep;
                }
            } else {
                log.info("Consumers are equal to nb partitions we can not scale up anymore");
            }
        }  //else if (totalArrivalRate < totalConsumptionRate) {
        //add another condition for lah check
        //to be done Estimated total arrival rate for next monitoring interval time
        else if (/*(totalArrivalRate *1000 < size *poll)*/ /*totalConsumptionRate *1000*/
                totallag  < ((size-1) *poll*waitingTime*dth)) /*totalConsumptionRate *1000*/ {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();

                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    // firstIteration = true;

                    scaled = true;
                    start = Instant.now();
                    firstIteration = true;
                    log.info("since   total lag  {} does not violate the SLA if I removed 1 consumer " +
                                    "(size-1) *poll*waitingTime,  I down scaled  by one {}",
                            totallag  , /*totalConsumptionRate *1000 */ /*size *poll*/ (size-1) *poll*waitingTime*dth);

//                log.info("since  rate   arrivals {} is smaller  than current   rate  consummed I down scaled  by one {}",
//                        totalArrivalRate *1000 /*15*/, totalConsumptionRate*1000/*15*/);

                    // recheck is this is needed....
                    //sleep = 2 * sleep
                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        } else {

            log.info("There was no scaling action in this iteration");
        }
        log.info("=================================:");



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



    static void scaleDecision5(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {


        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        long totallag = 0;

        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();

        log.info("Currently we have this number of consumers {}", size);


//        log.info("Logging current consumer rates:");
//        log.info("=================================:");
//
//
//        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
//            log.info("current maximum consumption rate for consumer {} is {}, ", memberDescription,
//                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
//        }
//
//        log.info("=================================:");


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


/*
            float consumptionRatePerConsumer =   ((float) (totalcoff - totalpoff) / sleep) * 1000.0f;
            float arrivalRatePerConsumer = ((float) (totalcoff - totalpoff) / sleep) * 1000.0f;*/

            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;


            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }

            log.info("=================================:");


          /*  log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    consumptionRatePerConsumer *1000);
            log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    arrivalRatePerConsumer *1000);*/

            if (consumptionRatePerConsumer > maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
            /*    log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer *1000,
                        maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) *1000);*/
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }


            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);




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
        double prediction =0;
        /////////////////////////////////////////////////prediction code
        if (iteration < 3) {

            log.info("No prediction iteration less than 4");
            log.info("Iteration = {}", iteration);
            log.info("Total arrivals since the last monitoring interval {}", (float) (totalArrivalRate * sleep));

            for (int j = 0; j < 3; j++)
                Xx[0][j] = Xx[0][j + 1];

            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate * sleep)));
            Xx[0][3] = y1;

            X = new Array2DRowRealMatrix(Xx);



            log.info("X = {}", X);

            log.info( " iteration {} Authorizations consumed  since the last monitoring interval {}", iteration,  (float) (totalConsumptionRate * sleep));
            //rls.add_obs(X.transpose(), y1);
            log.info( " iteration {}  the prediction regressor has not filled yet, iteration,  will set my prediction " +
                    "to the last received value and act accordingly. prediction = {} ", iteration, (float) (totalArrivalRate * sleep));
            prediction = (float) (totalArrivalRate * sleep);


        } else {

            log.info("Before y");
            double y1 = Double.parseDouble(String.valueOf(totalArrivalRate * sleep));
            log.info("After y");

            log.info("Total arrival  of authors since last interval {}", (float) (totalArrivalRate * sleep));


            for (int j = 0; j < 3; j++)
                Xx[0][j] = Xx[0][j + 1];
            Xx[0][3] = y1;


            X = new Array2DRowRealMatrix(Xx);

            log.info("X = {}", X);

            // rls.add_obs(X.transpose(), y1);

             prediction = (rls.getW().transpose().multiply(X.transpose())).getEntry(0, 0);

            log.info(" Iteration {} current total arrival since last monitoring interval {}, my prediction for the next one {}", iteration,
                    (float) (totalArrivalRate * sleep), prediction);

            log.info( " iteration {}  Authorizations consumed  since the last monitoring interval {}", iteration , (float) (totalConsumptionRate * sleep));
            log.info( " iteration {}  lag  observed  since the last monitoring interval {}", iteration , totallag);


            rls.add_obs(X.transpose(), y1);

        }
        iteration++;

        //we need the maximim total consulmer aret
        // when there is a lag it is operating at maximum

      /*  log.info(" total current  arrivals per seconds  {} , total  current  consumers  per seonds  {}",
                totalArrivalRate*1000, totalConsumptionRate*1000);*/
//        log.info("  total current  arrivals for the last 15 seconds {} , total   current  consumeed messages for last 15 messages  {}",
//                totalArrivalRate * 15 *1000, totalConsumptionRate * 15 * 1000);

        //if (totalArrivalRate * 15 * 1000 > 15 * size ) {
        // consumer is configured with maximum of messages per second consumption

        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate*1000, totalConsumptionRate*1000, totallag);

        log.info("shall we scale totalArrivalrate {}, max  consumption rate {}",
                totalArrivalRate *1000, /*size *poll **/(size *poll)/(float)SEC);

        //if ((totalArrivalRate *1000) > ((size *poll)/(float)SEC)) /*totalConsumptionRate *1000*/ {
            if ((Math.ceil(prediction)) > ((size *poll)/(float)SEC)) /*totalConsumptionRate *1000*/ {
            if (size < numberOfPartitions) {
                log.info("Consumers are less than nb partition we can scale");
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                    scaled = true;
                    start = Instant.now();
                    firstIteration = true;
            /*    log.info("since  arrivals in the last 15 secs  {} is greater than   consumed messages in the last 15,  I up scaled  by one {}",
                        totalArrivalRate * 1000 *15, totalConsumptionRate * 1000 *15);*/
                    log.info("since  arrivals prediction    {} is greater than  maximum consumption rate (size*poll/SEC) ,  I up scaled  by one {}",
                            totalArrivalRate * 1000, /*size *poll*/ /*totalConsumptionRate *1000*/(size *poll)/(float)SEC);
                    //TODO
                    // is that needed, ? a cool down period?
                    //sleep = 2 * sleep;
                }
            } else {
                log.info("Consumers are equal to nb partitions we can not scale up anymore");
            }


/*
            log.info("End Iteration scale up");
*/

        }  //else if (totalArrivalRate < totalConsumptionRate) {
        //add another condition for lah check
        //to be done Estimated total arrival rate for next monitoring interval time
        else if (/*(totalArrivalRate *1000 < size *poll)*/ /*totalConsumptionRate *1000*/
//                (totalArrivalRate *1000)  < (((size-1) *poll)/(float)SEC)) /*totalConsumptionRate *1000*/ {
                    (Math.ceil(prediction))  <= (((size-1) *poll)/(float)SEC)) {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    // firstIteration = true;

                    scaled = true;
                    firstIteration = true;
                    start = Instant.now();

                    log.info("since   arrivals prediction  {} is lower than max   consumption rate,   with size, I down scaled  by one {}",
                            totalArrivalRate * 1000, /*totalConsumptionRate *1000 */ /*size *poll*/
                            ((size) *poll)/(float)SEC);

//                log.info("since  rate   arrivals {} is smaller  than current   rate  consummed I down scaled  by one {}",
//                        totalArrivalRate *1000 /*15*/, totalConsumptionRate*1000/*15*/);

                    // recheck is this is needed....
                    //sleep = 2 * sleep
                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }

        log.info("quitting scale decision");

        log.info("=================================:");



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














