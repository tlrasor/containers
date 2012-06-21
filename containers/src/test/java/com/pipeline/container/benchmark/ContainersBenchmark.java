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
package com.pipeline.container.benchmark;

import java.util.Arrays;
import java.util.Iterator;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.AbstractIterator;
import com.pipeline.commons.util.ByteArray;
import com.pipeline.container.Container;
import com.pipeline.container.Containers;

/**
 * @author Travis Rasor
 */
public class ContainersBenchmark extends SimpleBenchmark {

    @Param({ "1", "8", "32", "128" }) int bufferedMemoryBufferSizeInMegabytes;
    @Param({ "1", "10", "100", "1024" }) int bufferedMemoryValueSizeInKiloBytes;

    public int timeBufferedMemoryContainerAppend(int reps) {
        Container container = Containers.newBufferedMemoryContainer(1024 * 1024 * bufferedMemoryBufferSizeInMegabytes);
        int size = 0;
        for (int i = 0; i < reps; i++) {
            size += runContainerTest(container, 1024 * bufferedMemoryValueSizeInKiloBytes);
        }
        return size;
    }

    @Param({ "1", "8", "32", "128" }) int mapSizeInMegabytes;
    @Param({ "1", "10", "100", "1024" }) int mapValueSizeInKiloBytes;

    public int timeInMemoryContainerAppend(int reps) {
        Container container = Containers.newMapContainer(1024 * 1024 * mapSizeInMegabytes);
        int size = 0;
        for (int i = 0; i < reps; i++) {
            size += runContainerTest(container, 1024 * mapValueSizeInKiloBytes);
        }
        return size;
    }

    @Param({ "8", "32", "128", "512", "1024" }) int fileBufferSizeInMegabytes;
    @Param({ "1", "10", "100", "1024" }) int fileValueSizeInKiloBytes;

    public int timeFileContainerAppend(int reps) {
        Container container = Containers.newFileContainer(1024 * 1024 * fileBufferSizeInMegabytes);
        int size = 0;
        for (int i = 0; i < reps; i++) {
            size += runContainerTest(container, 1024 * fileValueSizeInKiloBytes);
        }
        return size;
    }

    private static int runContainerTest(Container container, int valueSize) {
        Iterator<ByteArray> keys = byteIterator(16);
        Iterator<ByteArray> values = byteIterator(valueSize);
        while (container.canTake(valueSize)) {
            ByteArray key = keys.next();
            ByteArray val = values.next();
            container.append(key, val);
        }
        return container.size();
    }

    static Iterator<ByteArray> byteIterator(final int size) {
        return new AbstractIterator<ByteArray>() {
            int current;

            @Override
            protected ByteArray computeNext() {
                byte[] bytes = Arrays.copyOf(String.valueOf(current).getBytes(), size);
                current++;
                return ByteArray.from(bytes);
            }

        };
    }

    public static void main(String[] args) {
        Runner.main(ContainersBenchmark.class, args);
    }

}
