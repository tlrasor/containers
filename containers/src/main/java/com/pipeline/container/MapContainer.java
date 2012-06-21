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

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.pipeline.commons.util.ByteArray;

/**
 * Simpler class for testing, whatnot
 * 
 * @author Travis Rasor
 */
public class MapContainer implements Container {

    final Map<ByteArray, ByteArray> container;
    int bytes;
    int maxBytes;

    public MapContainer(int maxSizeInBytes) {
        this.maxBytes = maxSizeInBytes;
        this.container = Maps.newHashMap();
    }

    @Override
    public Optional<ByteArray> get(ByteArray key) {
        return Optional.fromNullable(container.get(key));
    }

    @Override
    public void append(ByteArray key, ByteArray data) {
        if (canTake(data.length())) {
            container.put(key, data);
            bytes += data.length();
        }

        else
            throw new RuntimeException("Container full");
    }

    @Override
    public boolean containsKey(ByteArray key) {
        return container.containsKey(key);
    }

    @Override
    public int size() {
        return container.size();
    }

    @Override
    public int sizeInBytes() {
        return bytes;
    }

    @Override
    public int maximumSizeInBytes() {
        return maxBytes;
    }

    @Override
    public boolean isEmpty() {
        return container.isEmpty();
    }

    @Override
    public boolean canTake(int size) {
        if (maxBytes - bytes >= size) return true;
        return false;
    }

    @Override
    public Set<ByteArray> keySet() {
        return container.keySet();
    }

}
