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
package net.consensys.orion.acceptance.send.receive.privacyGroup;

import static net.consensys.orion.acceptance.NodeUtils.createPrivacyGroupTransaction;
import static net.consensys.orion.acceptance.NodeUtils.findPrivacyGroupTransaction;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import net.consensys.cava.junit.TempDirectoryExtension;
import net.consensys.orion.acceptance.EthClientStub;
import net.consensys.orion.acceptance.NodeUtils;
import net.consensys.orion.acceptance.WaitUtils;
import net.consensys.orion.http.handler.privacy.PrivacyGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Runs up a two nodes that communicates with each other. */
@ExtendWith(TempDirectoryExtension.class)
class AddToPrivacyGroupTest extends PrivacyGroupAcceptanceTest {


  @Test
  void addToPrivacyGroup() {
    final EthClientStub firstNode = NodeUtils.client(firstOrionLauncher.clientPort(), firstHttpClient);
    final EthClientStub secondNode = NodeUtils.client(secondOrionLauncher.clientPort(), secondHttpClient);
    final EthClientStub thirdNode = NodeUtils.client(thirdOrionLauncher.clientPort(), thirdHttpClient);

    String name = "testName";
    String description = "testDescription";
    String[] addresses = new String[] {PK_1_B_64, PK_2_B_64};
    // create a privacy group
    final PrivacyGroup privacyGroup = createPrivacyGroupTransaction(firstNode, addresses, PK_1_B_64, name, description);

    String privacyGroupId = privacyGroup.getPrivacyGroupId();
    assertEquals(name, privacyGroup.getName());
    assertEquals(description, privacyGroup.getDescription());
    assertEquals(2, privacyGroup.getMembers().length);


    NodeUtils.addToPrivacyGroup(firstNode, new String[] {PK_3_B_64}, PK_1_B_64, privacyGroupId);

    String[] newGroupMembers = new String[] {PK_1_B_64, PK_2_B_64, PK_3_B_64};

    WaitUtils.waitFor(() -> {
      assertEquals(1, findPrivacyGroupTransaction(secondNode, newGroupMembers).length);
    });

    PrivacyGroup[] propagatedToExistingNode = findPrivacyGroupTransaction(secondNode, newGroupMembers);
    assertArrayEquals(newGroupMembers, propagatedToExistingNode[0].getMembers());
    assertEquals(privacyGroupId, propagatedToExistingNode[0].getPrivacyGroupId());

    PrivacyGroup[] propagatedToOriginalNode = findPrivacyGroupTransaction(firstNode, newGroupMembers);
    assertArrayEquals(newGroupMembers, propagatedToOriginalNode[0].getMembers());
    assertEquals(privacyGroupId, propagatedToOriginalNode[0].getPrivacyGroupId());

    PrivacyGroup[] propagatedToNewNode = findPrivacyGroupTransaction(thirdNode, newGroupMembers);
    assertArrayEquals(newGroupMembers, propagatedToNewNode[0].getMembers());
    assertEquals(privacyGroupId, propagatedToNewNode[0].getPrivacyGroupId());

  }

}