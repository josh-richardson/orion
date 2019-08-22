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

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import net.consensys.orion.enclave.CommitmentPair;
import net.consensys.orion.enclave.PrivacyGroupPayload;
import net.consensys.orion.enclave.QueryPrivacyGroupPayload;
import net.consensys.orion.exception.OrionErrorCode;
import net.consensys.orion.exception.OrionException;
import net.consensys.orion.storage.Storage;
import net.consensys.orion.utils.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

import static net.consensys.orion.http.server.HttpContentType.CBOR;

/**
 * Forward the private state of a privacy group to pantheon
 */
public class SetPrivacyGroupStateHandler implements Handler<RoutingContext> {

  private static final Logger log = LogManager.getLogger();
  private final Storage<PrivacyGroupPayload> privacyGroupStorage;

  public SetPrivacyGroupStateHandler(final Storage<PrivacyGroupPayload> privacyGroupStorage) {
    this.privacyGroupStorage = privacyGroupStorage;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void handle(final RoutingContext routingContext) {
    final byte[] request = routingContext.getBody().getBytes();
    final SetPrivacyGroupStateRequest setStateRequest = Serializer.deserialize(CBOR, SetPrivacyGroupStateRequest.class, request);

    privacyGroupStorage
        .get(setStateRequest.getPrivacyGroupId())
        .thenAccept((privacyGroupStateResult) -> {
            if (privacyGroupStateResult.isEmpty()) {
                routingContext
                        .fail(new OrionException(OrionErrorCode.ENCLAVE_PRIVACY_GROUP_MISSING, "privacy group not found"));
                return;
            }
            final ArrayList<CommitmentPair> privateState = setStateRequest.getPayload();


        })
        .exceptionally(
            e -> routingContext.fail(new OrionException(OrionErrorCode.ENCLAVE_UNABLE_STORE_PRIVACY_GROUP, e)));
  }
}
