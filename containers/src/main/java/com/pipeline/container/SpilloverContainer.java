/**
 * Copyright Jun 17, 2012 Travis Rasor
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

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.pipeline.commons.util.ByteArray;

/**
 * @author Travis Rasor
 */
public class SpilloverContainer implements Container {

    final Supplier<Container> containerSupplier;
    final Queue<Container> spillOverContainers;
    final Map<ByteArray, Container> containerMapping;
    Container currentContainer;
    int size;

    public SpilloverContainer(Container initialContainer, Supplier<Container> spilloverContainersSupplier) {
        this.currentContainer = initialContainer;
        this.containerSupplier = spilloverContainersSupplier;
        this.spillOverContainers = new ArrayDeque<>();
        this.containerMapping = Maps.newHashMap();
    }

    public Optional<ByteArray> get(ByteArray key) {
        Container container = containerMapping.get(key);
        if (container == null) return Optional.absent();
        return container.get(key);
    }

    public void append(ByteArray key, ByteArray data) {
        Container container = getWriteContainer(data.length());
        container.append(key, data);
        containerMapping.put(key, container);
        size++;
    }

    Container getWriteContainer(int writeSize) {
        if (currentContainer.canTake(writeSize)) return currentContainer;
        Container next = getNextContainer();
        return next;
    }

    private Container getNextContainer() {
        Container nextContainer = containerSupplier.get();
        spillOverContainers.add(currentContainer);
        currentContainer = nextContainer;
        return currentContainer;
    }

    public boolean containsKey(ByteArray key) {
        return containerMapping.containsKey(key);
    }

    public int size() {
        return size;
    }

    public int sizeInBytes() {
        throw new UnsupportedOperationException();
    }

    public int maximumSizeInBytes() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        if (currentContainer.isEmpty() && spillOverContainers.isEmpty()) return true;
        return false;
    }

    public boolean canTake(int size) {
        return true;
    }

    @Override
    public Set<ByteArray> keySet() {
        return containerMapping.keySet();
    }

}
