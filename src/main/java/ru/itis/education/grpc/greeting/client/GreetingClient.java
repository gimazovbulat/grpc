package ru.itis.education.grpc.greeting.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {

    private static Logger logger = LoggerFactory.getLogger(GreetingClient.class);

    String ipAddress = "localhost";
    int port = 5051;

    public static void main(String[] args) {
        logger.debug("gRPC Client is started");
        GreetingClient main = new GreetingClient();
        main.run();
    }

    private void run() {
        logger.debug("Channel is created on {} with port: {}", ipAddress, port);
        //gRPC provides a channel construct which abstracts out the underlying details like connection, connection pooling, load balancing, etc.
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ipAddress, port)
                .usePlaintext()
                .build();

        //examples how to initialize clients for gRPC
        //TestServiceGrpc.TestServiceBlockingStub syncClient = TestServiceGrpc.newBlockingStub(channel);
        //async client
        //TestServiceGrpc.TestServiceFutureStub asyncClient = TestServiceGrpc.newFutureStub(channel);

        //unary implementation
        doUnaryCall(channel);

        //server streaming implementation
        doServerStreamingCall(channel);

        //client streaming implementation
        doClientStreamingCall(channel);

        //bi directional streaming implementation
        doBiDiStreamingCall(channel);

        logger.debug("Shutting down channel");
        channel.shutdown();

    }


    private void doUnaryCall(ManagedChannel channel) {
        logger.debug("*** Unary implementation ***");
        //created a greet service client (blocking - sync)
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        //Unary
        //created a protocol buffer greeting message
        Greeting greeting = Greeting.newBuilder()
                .setFirstName("Ivan")
                .setLastName("Ivanov")
                .build();

        // the same for request
        GreetRequest greetRequest = GreetRequest.newBuilder()
                .setGreeting(greeting)
                .build();

        //call RPC call and get result
        GreetResponse greetResponse = greetClient.greet(greetRequest);

        logger.debug("Response has been received from server: - {}", greetResponse.getResult());
    }

    private void doServerStreamingCall(ManagedChannel channel) {
        logger.debug("*** Server streaming implementation ***");
        //created a greet service client (blocking - sync)
        GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);
        //Server Streaming
        GreetManyTimesRequest greetManyTimesRequest = GreetManyTimesRequest.newBuilder()
                .setGreeting(Greeting.newBuilder().setFirstName("Ivan"))
                .build();
        logger.debug("Send object with name - {}", greetManyTimesRequest);
        greetClient.greetmanyTimes(greetManyTimesRequest)
                .forEachRemaining(greetManyTimesResponse -> {
                    logger.debug("Response has been received from server: - {}", greetManyTimesResponse.getResult());
                });


    }

    private void doClientStreamingCall(ManagedChannel channel) {
        logger.debug("*** Client streaming implementation ***");
        //created a greet service client (async)
        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<LongGreetRequest> requestObserver = asyncClient.longGreet(new StreamObserver<LongGreetResponse>() {
            @Override
            public void onNext(LongGreetResponse longGreetResponse) {
                //we get a response from the server, onNext will be called only once
                logger.debug("Received a response from the server: {}", longGreetResponse.getResult());
            }

            @Override
            public void onError(Throwable throwable) {
                //we get an error from the server
            }

            @Override
            public void onCompleted() {
                //the server is done sending us data
                //onCompleted will be called right after onNext()
                logger.debug("Server has completed sending us something");
                latch.countDown();
            }
        });

        logger.debug("Sending message #1");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("Ivan")
                        .build())
                .build());

        logger.debug("Sending message #2");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("Petya")
                        .build())
                .build());

        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void doBiDiStreamingCall(ManagedChannel channel) {
        logger.debug("*** Bi directional streaming implementation ***");
        //created a greet service client (async)
        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GreetEveryoneRequest> requestObserver = asyncClient.greetEveryone(new StreamObserver<GreetEveryoneResponse>() {
            @Override
            public void onNext(GreetEveryoneResponse greetEveryoneResponse) {
                logger.debug("Response from the server: {}", greetEveryoneResponse.getResult());
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.debug("Server is done sending data");
                latch.countDown();
            }
        });

        Arrays.asList("Ivan", "Petya", "Lev").forEach(
                name -> {
                    logger.debug("Sending: {}", name);
                    requestObserver.onNext(GreetEveryoneRequest.newBuilder()
                            .setGreeting(Greeting.newBuilder()
                                    .setFirstName(name)
                                    .build())
                            .build());

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
