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
package net.consensys.orion.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static net.consensys.cava.io.Base64.encodeBytes;

import net.consensys.cava.bytes.Bytes;
import net.consensys.cava.concurrent.AsyncResult;
import net.consensys.cava.kv.KeyValueStore;
import net.consensys.orion.enclave.Enclave;
import net.consensys.orion.enclave.TransactionPair;
import net.consensys.orion.http.server.HttpContentType;
import net.consensys.orion.utils.Serializer;

import java.util.ArrayList;
import java.util.Optional;

import com.google.common.primitives.Longs;


public class PrivateTransactionStorage implements Storage<ArrayList<TransactionPair>> {

  private final KeyValueStore store;
  private final Enclave enclave;

  public PrivateTransactionStorage(KeyValueStore store, Enclave enclave) {
    this.store = store;
    this.enclave = enclave;
  }

  @Override
  public AsyncResult<String> put(ArrayList<TransactionPair> data) {
    String key = generateDigest(data);
    Bytes keyBytes = Bytes.wrap(key.getBytes(UTF_8));
    Bytes dataBytes = Bytes.wrap(Serializer.serialize(HttpContentType.CBOR, data));
    return store.putAsync(keyBytes, dataBytes).thenSupply(() -> key);
  }

  @Override
  public String generateDigest(ArrayList<TransactionPair> data) {
    return encodeBytes(Longs.toByteArray(data.hashCode()));
  }


  @Override
  public AsyncResult<Optional<ArrayList<TransactionPair>>> get(String key) {
    Bytes keyBytes = Bytes.wrap(key.getBytes(UTF_8));
    return store.getAsync(keyBytes).thenApply(
        maybeBytes -> Optional.ofNullable(maybeBytes).map(
            bytes -> Serializer.deserialize(HttpContentType.CBOR, ArrayList.class, bytes.toArrayUnsafe())));
  }

  @Override
  public AsyncResult<Optional<ArrayList<TransactionPair>>> update(String key, ArrayList<TransactionPair> data) {
    return get(key).thenApply((result) -> {
      Bytes keyBytes = Bytes.wrap(key.getBytes(UTF_8));
      Bytes dataBytes = Bytes.wrap(Serializer.serialize(HttpContentType.CBOR, data));
      store.putAsync(keyBytes, dataBytes);
      return Optional.of(data);
    });
  }
}