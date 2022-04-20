package ru.itis.education.grpc.greeting.client;

import com.google.common.util.concurrent.ListenableFuture;
import ru.itis.education.grpc.proto.calculation.CalculationServiceGrpc;
import ru.itis.education.grpc.proto.calculation.NumberRequest;
import ru.itis.education.grpc.proto.calculation.NumberResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CalculationClient {

    private static Logger logger = LoggerFactory.getLogger(GreetingClient.class);

    String ipAddress = "localhost";
    int port = 5051;

    public static void main(String[] args) {
        logger.debug("gRPC Client is started");
        CalculationClient main = new CalculationClient();
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
//
//        //client streaming implementation
        doClientStreamingCall(channel);
//
        //bi directional streaming implementation
        doBiDiStreamingCall(channel);
//
//        logger.debug("Shutting down channel");
        channel.shutdown();

    }


    private void doUnaryCall(ManagedChannel channel) {
        logger.debug("*** Unary implementation ***");
        //created a greet service client (blocking - sync)
        final CalculationServiceGrpc.CalculationServiceFutureStub calculationServiceFutureStub = CalculationServiceGrpc.newFutureStub(channel);


        final ListenableFuture<NumberResponse> responseListenableFuture = calculationServiceFutureStub.getSQRT(NumberRequest.newBuilder().setNumber(4d).build());

        try {
            logger.debug("Response has been received from server: {}", responseListenableFuture.get().getNumber());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void doServerStreamingCall(ManagedChannel channel) {
        final CalculationServiceGrpc.CalculationServiceBlockingStub calculationServiceFutureStub = CalculationServiceGrpc.newBlockingStub(channel);
        logger.debug("*** Server streaming implementation ***");
        //Server Streaming

        calculationServiceFutureStub.getDivisors(ru.itis.education.grpc.proto.calculation.NumberRequest.newBuilder().setNumber(40).build())
                .forEachRemaining(response -> {
                    logger.debug("Response has been received from server: - {}", response.getNumber());
                });

    }

    private void doClientStreamingCall(ManagedChannel channel) {
        logger.debug("*** Client streaming implementation ***");
        //created a greet service client (async)
        final CalculationServiceGrpc.CalculationServiceStub calculationServiceStub = CalculationServiceGrpc.newStub(channel);


        CountDownLatch latch = new CountDownLatch(1);

        final StreamObserver<NumberRequest> streamObserver = calculationServiceStub.getSTD(new StreamObserver<NumberResponse>() {


            @Override
            public void onNext(NumberResponse response) {
                logger.debug("Received a response from the server: {}", response.getNumber());
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


        Arrays.asList(2, 4, 4, 4, 5, 5, 7, 9)
                .forEach(number -> streamObserver.onNext(ru.itis.education.grpc.proto.calculation.NumberRequest.newBuilder().setNumber(number).build()));
        streamObserver.onCompleted();
        try {
            latch.await(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void doBiDiStreamingCall(ManagedChannel channel) {
        logger.debug("*** Bi directional streaming implementation ***");
        //created a greet service client (async)
        final CalculationServiceGrpc.CalculationServiceStub asyncClient = CalculationServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);
        final StreamObserver<NumberRequest> serverDataObserver = asyncClient.getUpdatedMax(new StreamObserver<NumberResponse>() {

            @Override
            public void onNext(NumberResponse numberResponse) {
                logger.debug("Response from the server: {}", numberResponse.getNumber());
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

        Arrays.asList(-5d, 4d, 3d).forEach(
                number -> {
                    logger.debug("Sending: {}", number);
                    serverDataObserver.onNext(ru.itis.education.grpc.proto.calculation.NumberRequest.newBuilder().setNumber(number).build());

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

        serverDataObserver.onCompleted();

        try {
            latch.await(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
