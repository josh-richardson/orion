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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.consensys.orion.enclave.CommitmentPair;
import net.consensys.orion.enclave.PrivacyGroupPayload;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Set the private state for a given privacy group ID
 */
public class SetPrivacyGroupStateRequest implements Serializable {
  private final ArrayList<CommitmentPair> payload;
  private final String privacyGroupId;

  @JsonCreator
  public SetPrivacyGroupStateRequest(
          @JsonProperty("privacyGroupId") String privacyGroupId,
          @JsonProperty("payload") ArrayList<CommitmentPair> payload
      ) {
    this.payload = payload;
    this.privacyGroupId = privacyGroupId;
  }

  @JsonProperty("payload")
  public ArrayList<CommitmentPair> getPayload() {
    return payload;
  }

  @JsonProperty("privacyGroupId")
  public String getPrivacyGroupId() {
    return privacyGroupId;
  }
}
