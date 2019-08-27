/*
 * Copyright 2019 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package net.consensys.orion.http.handler.set;

import static net.consensys.orion.http.server.HttpContentType.CBOR;
import static net.consensys.orion.http.server.HttpContentType.JSON;

import net.consensys.orion.config.Config;
import net.consensys.orion.enclave.PrivacyGroupPayload;
import net.consensys.orion.exception.OrionErrorCode;
import net.consensys.orion.exception.OrionException;
import net.consensys.orion.network.NodeHttpClientBuilder;
import net.consensys.orion.storage.Storage;
import net.consensys.orion.utils.Serializer;

import java.net.URL;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Forward the private state of a privacy group to pantheon
 */
public class SetPrivacyGroupStateHandler implements Handler<RoutingContext> {

  private static final Logger log = LogManager.getLogger();
  private final Storage<PrivacyGroupPayload> privacyGroupStorage;
  private final URL pantheonUrl;
  private final HttpClient httpClient;

  public SetPrivacyGroupStateHandler(
      final Storage<PrivacyGroupPayload> privacyGroupStorage,
      URL pantheonUrl,
      final Vertx vertx,
      final Config config) {
    this.privacyGroupStorage = privacyGroupStorage;
    this.pantheonUrl = pantheonUrl;
    this.httpClient = NodeHttpClientBuilder.build(vertx, config, 1500);
  }

  public void handle(final RoutingContext routingContext) {
    final byte[] request = routingContext.getBody().getBytes();
    final SetPrivacyGroupStateRequest setStateRequest =
        Serializer.deserialize(CBOR, SetPrivacyGroupStateRequest.class, request);

    privacyGroupStorage.get(setStateRequest.getPrivacyGroupId()).thenAccept((privacyGroupStateResult) -> {
      if (privacyGroupStateResult.isEmpty()) {
        routingContext.fail(new OrionException(OrionErrorCode.PANTHEON_URL_MISSING));
        return;
      }
      CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();

      httpClient
          .post(pantheonUrl.getPort(), pantheonUrl.getHost(), "/setPrivacyGroupInternalState")
          .putHeader("Content-Type", "application/json")
          .handler(response -> response.bodyHandler(responseBody -> {
            log.info("Pantheon with URL {} responded with {}", pantheonUrl.toString(), response.statusCode());
            if (response.statusCode() != 200) {
              responseFuture.completeExceptionally(new OrionException(OrionErrorCode.UNABLE_PUSH_TO_PANTHEON));
            } else {
              log.info("Success for /setPrivacyGroupInternalState");
              responseFuture.complete(true);
            }
          }))
          .exceptionHandler(
              ex -> responseFuture
                  .completeExceptionally(new OrionException(OrionErrorCode.UNABLE_PUSH_TO_PANTHEON, ex)))
          .end(Buffer.buffer(Serializer.serialize(JSON, setStateRequest)));

      responseFuture.whenComplete((all, ex) -> {
        if (ex != null) {
          log.error(ex);
          routingContext.fail(new OrionException(OrionErrorCode.UNABLE_PUSH_TO_PANTHEON));
          return;
        }
        final Buffer responseData = Buffer.buffer(Serializer.serialize(JSON, all));
        routingContext.response().end(responseData);
      });

    }).exceptionally(
        e -> routingContext.fail(new OrionException(OrionErrorCode.ENCLAVE_UNABLE_STORE_PRIVACY_GROUP, e)));
  }
}
