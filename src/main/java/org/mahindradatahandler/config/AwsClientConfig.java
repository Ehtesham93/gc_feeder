package org.mahindradatahandler.config;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import java.time.Duration;

public class AwsClientConfig {
        private static final int MAX_RETRIES = 5;
        private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(10);
        private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);
        private static final int MAX_CONNECTIONS = 100;
        private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);
        private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(60);

        public static KinesisAsyncClient createKinesisClient() {
                return KinesisAsyncClient.builder()
                                .httpClient(createHttpClient())
                                .overrideConfiguration(createOverrideConfig())
                                .build();
        }

        public static SdkAsyncHttpClient createHttpClient() {
                return NettyNioAsyncHttpClient.builder()
                                .maxConcurrency(MAX_CONNECTIONS)
                                .connectionTimeout(CONNECTION_TIMEOUT)
                                .readTimeout(READ_TIMEOUT)
                                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
                                .connectionMaxIdleTime(IDLE_TIMEOUT)
                                .http2Configuration(Http2Configuration.builder()
                                                .maxStreams((long) MAX_CONNECTIONS)
                                                .healthCheckPingPeriod(Duration.ofSeconds(5))
                                                .build())
                                .build();
        }

        public static ClientOverrideConfiguration createOverrideConfig() {
                return ClientOverrideConfiguration.builder()
                                .retryPolicy(RetryPolicy.builder()
                                                .numRetries(MAX_RETRIES)
                                                .retryCondition(RetryCondition.defaultRetryCondition())
                                                .backoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
                                                .build())
                                .build();
        }
}
