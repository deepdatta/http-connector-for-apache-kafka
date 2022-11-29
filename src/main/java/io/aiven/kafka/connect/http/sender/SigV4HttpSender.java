/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http.sender;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;   



final class SigV4HttpSender extends HttpSender {

    private final Logger logger = LoggerFactory.getLogger(SigV4HttpSender.class);

    private Aws4Signer awsSigner;
    private Region awsRegion;
    private String awsService;
    private DefaultCredentialsProvider credentialProvider;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private void init(final HttpSinkConfig config) {
        awsRegion = Region.of(Objects.requireNonNull(config.awsIamRegion()));
        awsService = Objects.requireNonNull(config.awsIamService());
        credentialProvider = DefaultCredentialsProvider.create();
        awsSigner = Aws4Signer.create();
    }

    SigV4HttpSender(final HttpSinkConfig config) {
        super(config);
        init(config);
    }

    //for testing
    SigV4HttpSender(final HttpSinkConfig config, final HttpClient httpClient) {
        super(config, httpClient);
        init(config);
    }

    @Override
    protected HttpResponse<String> sendWithRetries(final HttpRequest.Builder requestBuilder,
                                                   final HttpResponseHandler originHttpResponseHandler) {
        final HttpRequest.Builder signedRequestBuilder = signRequest(requestBuilder);
        return super.sendWithRetries(signedRequestBuilder, originHttpResponseHandler);
    }

    private HttpRequest.Builder signRequest(final HttpRequest.Builder requestBuilder) {
        final HttpRequest request = requestBuilder.build();
        
        /*** copy Java.net HttpRequest to AWS request builder ***/
        final SdkHttpFullRequest.Builder awsRequestBuilder = SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.fromValue(request.method()))
            .uri(request.uri());
        
        // Copy Headers
        awsRequestBuilder.headers(request.headers().map());
  
        // Check for payload
        copyPayload(request, awsRequestBuilder);

        /*** Sign ***/
        final SdkHttpFullRequest signedRequest = signRequest(awsRequestBuilder.build());

        /*** Copy everything back to the Java.net request builder ***/
        final HttpRequest.Builder signedRequestBuilder = HttpRequest.newBuilder(signedRequest.getUri());

        // Copy Headers
        for (final Map.Entry<String, List<String>> headerEntry : signedRequest.headers().entrySet()) {
            for (final String value : headerEntry.getValue()) {
                signedRequestBuilder.header(headerEntry.getKey(), value);
            }
        }

        // Copy Method and Payload
        final InputStream payload = signedRequest.contentStreamProvider()
            .orElseThrow(() -> new IllegalStateException("There must be content"))
            .newStream();
        signedRequestBuilder.method(request.method(), HttpRequest.BodyPublishers.ofInputStream(() -> payload));

        return signedRequestBuilder;
    }

    /**
     * Copies the HTTP payload from request to awsRequestBuilder
     * @param request Source
     * @param awsRequestBuilder Destination
     */
    private void copyPayload(final HttpRequest request, final SdkHttpFullRequest.Builder awsRequestBuilder) {
        if (request.bodyPublisher().isPresent()) {
            final var bodyPublisher = request.bodyPublisher().get();
            if (bodyPublisher.contentLength() == 0) {
                return;
            }

            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            final BodySubscriber bodySubscriber = new BodySubscriber(outputStream);
            bodyPublisher.subscribe(bodySubscriber);
            awsRequestBuilder.contentStreamProvider(() -> 
                new ByteArrayInputStream(outputStream.toByteArray()));
        } 
    }

    private SdkHttpFullRequest signRequest(final SdkHttpFullRequest request) {
        final ExecutionAttributes attributes = new ExecutionAttributes();
        attributes.putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS,
                                credentialProvider.resolveCredentials());
        attributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME, awsService);
        attributes.putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, awsRegion);
        return awsSigner.sign(request, attributes);
    }

    private static class BodySubscriber implements Flow.Subscriber<ByteBuffer> {
        
        final ByteArrayOutputStream bodyBytes;
        
        BodySubscriber(final ByteArrayOutputStream bodyBytes) {
            this.bodyBytes = bodyBytes;
        }

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {
            /* empty */
        }

        @Override
        public void onNext(final ByteBuffer item) {
            bodyBytes.writeBytes(item.array());
        }

        @Override
        public void onError(final Throwable throwable) {
            /* empty */
        }

        @Override
        public void onComplete() { 
            /* empty */
        }
    }

}
