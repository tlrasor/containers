/**
 * Copyright Jun 18, 2012 Travis Rasor
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.pipeline.container;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.pipeline.commons.util.ByteArray;

/**
 * @author Travis Rasor
 */
public class SpilloverContainerTest {

    SpilloverContainer container;

    @Test
    public void testFillMultipleContainers() {
        Container intialContainer = newArraySupplier.get();
        container = new SpilloverContainer(intialContainer, newArraySupplier);

        byte[] k1 = new byte[] { 1 };
        byte[] k2 = new byte[] { 2 };
        byte[] k3 = new byte[] { 3 };
        byte[] k4 = new byte[] { 4 };
        byte[] k5 = new byte[] { 5 };
        byte[] v1 = new byte[50];

        container.append(ByteArray.from(k1), ByteArray.from(v1));
        assertTrue(container.currentContainer == intialContainer);
        assertTrue(container.spillOverContainers.isEmpty());

        container.append(ByteArray.from(k2), ByteArray.from(v1));
        container.append(ByteArray.from(k3), ByteArray.from(v1));
        assertTrue(container.spillOverContainers.size() == 1);
        container.append(ByteArray.from(k4), ByteArray.from(v1));
        container.append(ByteArray.from(k5), ByteArray.from(v1));
        assertTrue(container.spillOverContainers.size() == 2);

    }

    Supplier<Container> newArraySupplier = new Supplier<Container>() {

        @Override
        public Container get() {
            return new MapContainer(100);
        }
    };
}
