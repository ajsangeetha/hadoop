/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.Resource;

/**
 * This data structure stores a periodic RLESparseResourceAllocation.
 * Default period is 1 day (86400000ms).
 */

public class PeriodicRLESparseResourceAllocation {

  private RLESparseResourceAllocation rleVector;
  long timePeriod;

  public PeriodicRLESparseResourceAllocation(
      RLESparseResourceAllocation rleVector, Long timePeriod) {
    this.rleVector = rleVector;
    this.timePeriod = timePeriod;
  }

  public PeriodicRLESparseResourceAllocation(
      RLESparseResourceAllocation rleVector) {
    this(rleVector, 86400000L);
  }

  /**
   * Get capacity at time based on periodic repetition.
   *
   * @param tick UTC time for which the allocated {@link Resource} is queried.
   * @return {@link Resource} allocated at specified time
   */
  public Resource getCapacityAtTime(long tick) {
    long convertedTime = (tick % timePeriod);
    return rleVector.getCapacityAtTime(convertedTime);
  }

  /**
   * Add resource for the specified interval. This function will be used by
   * {@link InMemoryPlan} while placing reservations between 0 and timePeriod.
   *
   * @param interval {@link ReservationInterval} to which the specified
   *          resource is to be added.
   * @param resource {@link Resource} to be added to the interval  specified.
   */
  public void setCapacityInInterval(ReservationInterval interval,
      Resource resource) {
    if (interval.getEndTime() <= timePeriod) {
      rleVector.addInterval(interval, resource);
    } else {
      System.out.println("Cannot set capacity beyond end time: " + timePeriod);
    }
  }

   /**
   * Removes a resource for the specified interval.
   *
   * @param interval the {@link ReservationInterval} for which the resource is
   *          to be removed.
   * @param resource the {@link Resource} to be removed.
   */
  public void removeInterval(ReservationInterval interval, Resource resource) {
    if (interval.getEndTime() <= timePeriod) {
      rleVector.removeInterval(interval, resource);
    } else {
      System.out.println("Interval extends beyond the end time " + timePeriod);
    }
  }

  /**
   * Get maximum capacity at periodic offsets from the specified time.
   *
   * @param tick UTC time base from which offsets are specified for finding 
   *          the maximum capacity.
   * @param period periodic offset at which capacities are evaluted.
   * @return the maximum {@link Resource} across the specified time instants. 
   */
  public Resource getMaxPeriodicCapacity(long tick, long period) {
    Resource maxResource;
    if (period < timePeriod) {
      maxResource = rleVector.getMaxPeriodicCapacity(
          tick % timePeriod, period);
    } else {
      // if period is greater than the length of PeriodicRLESparseAllocation,
      // only a single value exists in this interval.
      maxResource = rleVector.getCapacityAtTime(tick % timePeriod);
    }
    return maxResource;
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append("Period: ").append(timePeriod).append("\n")
        .append(rleVector.toString());
    if (rleVector.isEmpty()) {
      ret.append(" no allocations\n");
    }
    return ret.toString();
  }

}
