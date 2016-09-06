/******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *****************************************************************************/

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing the class PeriodicRLESparseResourceAllocation.
 */
public class TestPeriodicRLESparseResourceAllocation {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestPeriodicRLESparseResourceAllocation.class);

  @Test
  public void testPeriodicCapacity() {
    int[] alloc = {10, 7, 5, 2, 0};
    long[] timeSteps = {0L, 5L, 10L, 15L, 19L};
    RLESparseResourceAllocation rleSparseVector =
        generateAllocations(alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodicVector =
        new PeriodicRLESparseResourceAllocation(rleSparseVector, 20L);
    LOG.info(periodicVector.toString());
    Assert.assertEquals(Resource.newInstance(5, 5),
        periodicVector.getCapacityAtTime(10L));
    Assert.assertEquals(Resource.newInstance(10, 10),
        periodicVector.getCapacityAtTime(20L));
    Assert.assertEquals(Resource.newInstance(5, 5),
        periodicVector.getCapacityAtTime(50L));
  }

  @Test
  public void testMaxPeriodicCapacity() {
    int[] alloc = {2, 5, 7, 10, 3, 4, 6, 8};
    long[] timeSteps = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L};
    RLESparseResourceAllocation rleSparseVector =
        ReservationSystemTestUtil.generateRLESparseResourceAllocation(
            alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodicVector =
        new PeriodicRLESparseResourceAllocation(rleSparseVector, 8L);
    LOG.info(periodicVector.toString());
    Assert.assertEquals(
        periodicVector.getMaxPeriodicCapacity(0, 1),
        Resource.newInstance(10, 10));
    Assert.assertEquals(
        periodicVector.getMaxPeriodicCapacity(8, 2),
        Resource.newInstance(7, 7));
    Assert.assertEquals(
        periodicVector.getMaxPeriodicCapacity(16, 3),
        Resource.newInstance(10, 10));
    Assert.assertEquals(
        periodicVector.getMaxPeriodicCapacity(17, 4),
        Resource.newInstance(5, 5));
    Assert.assertEquals(
        periodicVector.getMaxPeriodicCapacity(32, 5),
        Resource.newInstance(4, 4));
  }

}
