package ru.itis.education.grpc.greeting.server;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreetServiceImpl extends GreetServiceGrpc.GreetServiceImplBase {

    private static Logger logger = LoggerFactory.getLogger(GreetServiceImpl.class);

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        logger.debug("*** Unary implementation on server side ***");
        Greeting greeting = request.getGreeting();
        logger.debug("Request has been received on server side: firstName - {}, lastName - {}", greeting.getFirstName(), greeting.getLastName());

        String firstName = greeting.getFirstName();
        String result = "Hello " + firstName;

        GreetResponse response = GreetResponse.newBuilder()
                .setResult(result)
                .build();

        //send the response
        responseObserver.onNext(response);

        //complete RPC call
        responseObserver.onCompleted();
    }

    @Override
    public void greetmanyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
        logger.debug("*** Server streaming implementation on server side ***");
        Greeting greeting = request.getGreeting();
        String firstName = greeting.getFirstName();

        try {
            for (int i = 0; i < 10; i++) {
                String result = "Hello " + firstName + ", response number:" + (i + 1);
                GreetManyTimesResponse response = GreetManyTimesResponse.newBuilder()
                        .setResult(result)
                        .build();
                logger.debug("send response {} of 10", i + 1);
                responseObserver.onNext(response);
                Thread.sleep(1000L);

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            logger.debug("all messages have been sent");
            responseObserver.onCompleted();

        }

    }

    @Override
    public StreamObserver<LongGreetRequest> longGreet(StreamObserver<LongGreetResponse> responseObserver) {
        logger.debug("*** Client streaming implementation on server side ***");
        StreamObserver<LongGreetRequest> streamObserverofRequest = new StreamObserver<LongGreetRequest>() {

            String result = "";

            @Override
            public void onNext(LongGreetRequest longGreetRequest) {
                logger.debug("make some calculation for each request");
                //client sends a message
                result += ". Hello " + longGreetRequest.getGreeting().getFirstName() + "!";
            }

            @Override
            public void onError(Throwable throwable) {
                //client sends an error
            }

            @Override
            public void onCompleted() {
//                client is done, this is when we want to return a response (responseObserver)
                responseObserver.onNext(LongGreetResponse.newBuilder()
                        .setResult(result)
                        .build());
                logger.debug("Send result: - {}",result);
                responseObserver.onCompleted();
            }
        };

        return streamObserverofRequest;
    }


    @Override
    public StreamObserver<GreetEveryoneRequest> greetEveryone(StreamObserver<GreetEveryoneResponse> responseObserver) {
        logger.debug("*** Bi directional streaming implementation on server side ***");

        StreamObserver<GreetEveryoneRequest> requestObserver = new StreamObserver<GreetEveryoneRequest>() {
            @Override
            public void onNext(GreetEveryoneRequest value) {
                //client sends a message
                String result = "Hello " + value.getGreeting().getFirstName();
                GreetEveryoneResponse greetEveryoneResponse = GreetEveryoneResponse.newBuilder()
                        .setResult(result)
                        .build();

                //send message for each request
                logger.debug("Send result for each request: - {}",result);
                responseObserver.onNext(greetEveryoneResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                //nothing
            }

            @Override
            public void onCompleted() {
                //client is done, so complete server-side also
                logger.debug("close bi directional streaming");
                responseObserver.onCompleted();
            }
        };

        return requestObserver;
    }
}
