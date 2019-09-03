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
package net.consensys.orion.http.handler.privacy;

import static net.consensys.orion.http.server.HttpContentType.JSON;

import net.consensys.cava.concurrent.AsyncResult;
import net.consensys.cava.crypto.sodium.Box;
import net.consensys.orion.config.Config;
import net.consensys.orion.enclave.CommitmentPair;
import net.consensys.orion.enclave.CommitmentTriplet;
import net.consensys.orion.enclave.Enclave;
import net.consensys.orion.enclave.EnclaveException;
import net.consensys.orion.enclave.EncryptedPayload;
import net.consensys.orion.enclave.PrivacyGroupPayload;
import net.consensys.orion.enclave.QueryPrivacyGroupPayload;
import net.consensys.orion.exception.OrionErrorCode;
import net.consensys.orion.exception.OrionException;
import net.consensys.orion.http.handler.set.SetPrivacyGroupRequest;
import net.consensys.orion.http.handler.set.SetPrivacyGroupStateRequest;
import net.consensys.orion.network.ConcurrentNetworkNodes;
import net.consensys.orion.network.NodeHttpClientBuilder;
import net.consensys.orion.storage.Storage;
import net.consensys.orion.utils.Serializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Add a member to a privacy group
 */
public class AddToPrivacyGroupHandler extends PrivacyGroupBaseHandler implements Handler<RoutingContext> {

  private static final Logger log = LogManager.getLogger();

  private final Storage<PrivacyGroupPayload> privacyGroupStorage;
  private final Storage<QueryPrivacyGroupPayload> queryPrivacyGroupStorage;
  private final Storage<ArrayList<CommitmentPair>> privateTransactionStorage;
  private final Storage<EncryptedPayload> storage;
  private final Enclave enclave;


  public AddToPrivacyGroupHandler(
      final Storage<PrivacyGroupPayload> privacyGroupStorage,
      final Storage<QueryPrivacyGroupPayload> queryPrivacyGroupStorage,
      final Storage<ArrayList<CommitmentPair>> privateTransactionStorage,
      final Storage<EncryptedPayload> storage,
      final ConcurrentNetworkNodes networkNodes,
      final Enclave enclave,
      final Vertx vertx,
      final Config config) {
    super(networkNodes, NodeHttpClientBuilder.build(vertx, config, 1500));
    this.privacyGroupStorage = privacyGroupStorage;
    this.queryPrivacyGroupStorage = queryPrivacyGroupStorage;
    this.privateTransactionStorage = privateTransactionStorage;
    this.storage = storage;
    this.enclave = enclave;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final byte[] request = routingContext.getBody().getBytes();
    final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest =
        Serializer.deserialize(JSON, ModifyPrivacyGroupRequest.class, request);

    privacyGroupStorage.get(modifyPrivacyGroupRequest.privacyGroupId()).thenAccept((result) -> {
      if (result.isEmpty() || result.get().state() == PrivacyGroupPayload.State.DELETED) {
        routingContext
            .fail(new OrionException(OrionErrorCode.ENCLAVE_PRIVACY_GROUP_MISSING, "privacy group not found"));
        return;
      }

      final PrivacyGroupPayload oldPrivacyGroupPayload = result.get();
      if (!Arrays.asList(oldPrivacyGroupPayload.addresses()).contains(modifyPrivacyGroupRequest.from())) {
        routingContext
            .fail(new OrionException(OrionErrorCode.ENCLAVE_PRIVACY_GROUP_MISSING, "privacy group not found"));
        return;
      }

      PrivacyGroupPayload combinedPrivacyGroup = new PrivacyGroupPayload(
          getCombinedAddresses(modifyPrivacyGroupRequest, oldPrivacyGroupPayload),
          oldPrivacyGroupPayload.name(),
          oldPrivacyGroupPayload.description(),
          oldPrivacyGroupPayload.state(),
          oldPrivacyGroupPayload.type(),
          oldPrivacyGroupPayload.randomSeed());

      final List<CompletableFuture<Boolean>> addRequests =
          addToPrivacyGroupOtherMembers(modifyPrivacyGroupRequest, combinedPrivacyGroup);

      CompletableFuture.allOf(addRequests.toArray(CompletableFuture[]::new)).whenComplete((all, ex) -> {
        if (ex != null) {
          handleFailure(routingContext, ex);
          return;
        }

        /*
         * Todo here: get ordered list of commitments and private transactions from `PrivateTransactionStore` to push to Pantheon
         * */

        addToPrivacyGroupInternal(
            routingContext,
            modifyPrivacyGroupRequest,
            oldPrivacyGroupPayload,
            combinedPrivacyGroup);
      });
    }).exceptionally(
        e -> routingContext.fail(new OrionException(OrionErrorCode.ENCLAVE_UNABLE_STORE_PRIVACY_GROUP, e)));
  }

  @NotNull
  private String[] getCombinedAddresses(
      final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest,
      final PrivacyGroupPayload oldPrivacyGroupPayload) {
    return Stream
        .concat(Arrays.stream(oldPrivacyGroupPayload.addresses()), Stream.of(modifyPrivacyGroupRequest.address()))
        .distinct()
        .toArray(String[]::new);
  }

  private List<CompletableFuture<Boolean>> addToPrivacyGroupOtherMembers(
      final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest,
      final PrivacyGroupPayload combinedPrivacyGroup) {
    Stream<Box.PublicKey> combinedAddresses = Arrays
        .stream(combinedPrivacyGroup.addresses())
        .filter(key -> !key.equals(modifyPrivacyGroupRequest.from()))
        .distinct()
        .map(enclave::readKey);
    return sendRequestsToOthers(
        combinedAddresses,
        new SetPrivacyGroupRequest(combinedPrivacyGroup, modifyPrivacyGroupRequest.privacyGroupId()),
        "/setPrivacyGroup");
  }

  private void addToPrivacyGroupInternal(
      final RoutingContext routingContext,
      final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest,
      final PrivacyGroupPayload oldPrivacyGroupPayload,
      final PrivacyGroupPayload innerCombinedPrivacyGroupPayload) {
    privacyGroupStorage
        .update(privacyGroupStorage.generateDigest(oldPrivacyGroupPayload), innerCombinedPrivacyGroupPayload)
        .thenAccept((privacyGroupResult) -> {
          updateQueryPrivacyGroupStorage(routingContext, modifyPrivacyGroupRequest, innerCombinedPrivacyGroupPayload);
        })
        .exceptionally(
            e -> routingContext.fail(new OrionException(OrionErrorCode.ENCLAVE_UNABLE_STORE_PRIVACY_GROUP, e)));
  }

  private void updateQueryPrivacyGroupStorage(
      final RoutingContext routingContext,
      final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest,
      final PrivacyGroupPayload innerCombinedPrivacyGroupPayload) {
    final QueryPrivacyGroupPayload queryPrivacyGroupPayload =
        new QueryPrivacyGroupPayload(innerCombinedPrivacyGroupPayload.addresses(), null);
    queryPrivacyGroupPayload.setPrivacyGroupToModify(modifyPrivacyGroupRequest.privacyGroupId());
    final String key = queryPrivacyGroupStorage.generateDigest(queryPrivacyGroupPayload);
    log.info("Stored privacy group. resulting digest: {}", key);
    queryPrivacyGroupStorage.update(key, queryPrivacyGroupPayload).thenAccept((queryPrivacyGroupStorageResult) -> {
      buildPrivacyGroupResponse(routingContext, modifyPrivacyGroupRequest, innerCombinedPrivacyGroupPayload);
    }).exceptionally(
        e -> routingContext.fail(new OrionException(OrionErrorCode.ENCLAVE_UNABLE_STORE_PRIVACY_GROUP, e)));
  }

  private void buildPrivacyGroupResponse(
      final RoutingContext routingContext,
      final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest,
      final PrivacyGroupPayload innerCombinedPrivacyGroupPayload) {
    final PrivacyGroup group = new PrivacyGroup(
        modifyPrivacyGroupRequest.privacyGroupId(),
        PrivacyGroupPayload.Type.PANTHEON,
        innerCombinedPrivacyGroupPayload.name(),
        innerCombinedPrivacyGroupPayload.description(),
        innerCombinedPrivacyGroupPayload.addresses());
    log.info("Storing privacy group {} complete", modifyPrivacyGroupRequest.privacyGroupId());
    final Buffer toReturn = Buffer.buffer(Serializer.serialize(JSON, group));
    propagatePrivacyGroupState(routingContext, modifyPrivacyGroupRequest, toReturn);
  }

  private void propagatePrivacyGroupState(
      final RoutingContext routingContext,
      final ModifyPrivacyGroupRequest modifyPrivacyGroupRequest,
      final Buffer toReturn) {
    privateTransactionStorage.get(modifyPrivacyGroupRequest.privacyGroupId()).thenAccept(resultantState -> {
      if (resultantState.isPresent()) {
        var commitmentPairs = resultantState.get();

        var newPairs = commitmentPairs.stream().map(p -> {
          try {
            var optionalEncryptedPayload = storage.get(p.enclaveKey()).get();
            if (optionalEncryptedPayload.isPresent()) {
              var encryptedPayload = optionalEncryptedPayload.get();
              return new CommitmentTriplet(p.enclaveKey(), p.markerTxHash(), encryptedPayload.nonce());
            }
          } catch (InterruptedException ignored) {
          }
          return null;
        }).filter(Objects::nonNull).toArray();

        var triplets = new ArrayList<CommitmentTriplet>();



        SetPrivacyGroupStateRequest setGroupStateRequest =
            new SetPrivacyGroupStateRequest(modifyPrivacyGroupRequest.privacyGroupId(), triplets);
        var setPrivateStateRequests = sendRequestsToOthers(
            Stream.of(enclave.readKey(modifyPrivacyGroupRequest.address())),
            setGroupStateRequest,
            "/setPrivacyGroupState");
        CompletableFuture.allOf(setPrivateStateRequests.toArray(CompletableFuture[]::new)).whenComplete((all, ex) -> {
          if (ex != null) {
            handleFailure(routingContext, ex);
            return;
          }
          routingContext.response().end(toReturn);
        });
      } else {
        routingContext.response().end(toReturn);
      }
    });
  }

  /*todo: return a stream of this and then wrap in CompletableFuture.allOf, whenComplete*/
//  private AsyncResult<ArrayList<CommitmentTriplet>> retrieveCommitmentTriplets(
//      final ArrayList<CommitmentPair> resultantState) throws InterruptedException {
//    var triplets = new ArrayList<CommitmentTriplet>();
//    var futureResults = new ArrayList<AsyncResult<Optional<EncryptedPayload>>>();
//    for (final CommitmentPair commitmentPair : resultantState) {
//      futureResults.add(storage.get(commitmentPair.enclaveKey()));
//
//      storage.get(commitmentPair.enclaveKey()).thenAccept(storageItem -> {
//        if (storageItem != null && storageItem.isPresent()) {
//          byte[] decryptedPayload;
//          try {
//            decryptedPayload = enclave.decrypt(storageItem.get(), enclave.nodeKeys()[0]);
//            triplets.add(
//                new CommitmentTriplet(commitmentPair.enclaveKey(), commitmentPair.markerTxHash(), decryptedPayload));
//          } catch (EnclaveException e) {
//          }
//        } else {
//        }
//      });
//
//    }
//    return triplets;
//  }
}
