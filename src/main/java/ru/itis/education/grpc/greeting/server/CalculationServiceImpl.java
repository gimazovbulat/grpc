package ru.itis.education.grpc.greeting.server;

import ru.itis.education.grpc.proto.calculation.NumberRequest;
import ru.itis.education.grpc.proto.calculation.NumberResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CalculationServiceImpl extends CalculationServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(CalculationServiceImpl.class);


    @Override
    public void getSQRT(NumberRequest request, StreamObserver<NumberResponse> responseObserver) {
        final double number = request.getNumber();
        final NumberResponse numberResponse = NumberResponse.newBuilder().setNumber(Math.sqrt(number)).build();
        responseObserver.onNext(numberResponse);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<NumberRequest> getSTD(StreamObserver<NumberResponse> responseObserver) {
        final StreamObserver<NumberRequest> numberRequestStreamObserver = new StreamObserver<>() {
            private final List<Double> numberList = new ArrayList<>();

            @Override
            public void onNext(NumberRequest numberRequest) {
                final double number = numberRequest.getNumber();
                logger.info("Received : {}", number);
                numberList.add(number);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                final double sum = numberList.stream().mapToDouble(Double::doubleValue).sum();
                double middle = sum / numberList.size();
                final double sum2 = numberList.stream().map(number -> Math.pow(number - middle, 2))
                        .mapToDouble(Double::doubleValue).sum();
                final double result = Math.sqrt(sum2 / numberList.size());
                logger.debug("Send result: - {}", result);

                responseObserver.onNext(NumberResponse.newBuilder().setNumber(result).build());
                responseObserver.onCompleted();
            }
        };
        return numberRequestStreamObserver;
    }

    @Override
    public void getDivisors(NumberRequest request, StreamObserver<NumberResponse> responseObserver) {
        final double number = request.getNumber();
        double sqrt = Math.sqrt(number);
        double currentValue = number;
        double multiplier = 2;
        while (currentValue > 1 && multiplier <= sqrt) {
            if (currentValue % multiplier == 0) {
                NumberResponse response = NumberResponse.newBuilder().setNumber((double) multiplier).build();
                responseObserver.onNext(response);
                currentValue /= multiplier;
            } else if (multiplier == 2) {
                multiplier++;
            } else {
                multiplier += 2;
            }
        }
        if (currentValue != 1) {
            NumberResponse response = NumberResponse.newBuilder().setNumber((double) currentValue).build();
            responseObserver.onNext(response);
        }
        if (number <= 0) {
            responseObserver.onError(new IllegalArgumentException("Under 0"));
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<NumberRequest> getUpdatedMax(StreamObserver<NumberResponse> responseObserver) {
        final StreamObserver<NumberRequest> numberRequestStreamObserver = new StreamObserver<>() {
            private double currentMax = -10000000d;

            @Override
            public void onNext(NumberRequest numberRequest) {
                final double number = numberRequest.getNumber();
                logger.info("Received : {}", number);
                if (currentMax < number) {
                    currentMax = number;
                }
                responseObserver.onNext(NumberResponse.newBuilder().setNumber(currentMax).build());

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

        };
        return numberRequestStreamObserver;
    }
}
