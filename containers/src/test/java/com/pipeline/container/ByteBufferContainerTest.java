/**
 * Copyright Jun 16, 2012 Travis Rasor
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.base.Optional;
import com.pipeline.commons.util.ByteArray;
import com.pipeline.conditions.ConditionViolationException;

/**
 * @author Travis Rasor
 */
public class ByteBufferContainerTest {

    @Test
    public void testAppend() {
        Container container = Containers.newBufferedMemoryContainer(1024 * 1024);
        ByteArray key = ByteArray.from(new byte[] { 8, 8, 88 });
        ByteArray value = ByteArray.from(new byte[] { 8, 9, 10 });
        container.append(key, value);
        assertTrue(container.containsKey(key));
        assertEquals(1, container.size());
        assertEquals(3, container.sizeInBytes());

        ByteArray key2 = ByteArray.from(new byte[] { 3, 2, 1 });
        ByteArray value2 = ByteArray.from(new byte[1024 * 1024 - 3]);

        assertTrue(container.canTake(value.getArray().length));
        container.append(key2, value2);
        assertTrue(container.containsKey(key2));
        assertEquals(container.maximumSizeInBytes(), container.sizeInBytes());
        assertEquals(2, container.size());
    }

    @Test(expected = ConditionViolationException.class)
    public void testAppendMultipleKeys() {
        Container container = Containers.newBufferedMemoryContainer(1024 * 1024);
        ByteArray key = ByteArray.from(new byte[] { 8, 8, 88 });
        ByteArray value = ByteArray.from(new byte[] { 8, 9, 10 });
        container.append(key, value);
        assertTrue(container.containsKey(key));

        ByteArray value2 = ByteArray.from(new byte[10]);
        container.append(key, value2);

    }

    @Test(expected = ConditionViolationException.class)
    public void testAppendOverSize() {
        Container container = Containers.newBufferedMemoryContainer(1024);
        ByteArray key = ByteArray.from(new byte[] { 8, 8, 88 });
        ByteArray value = ByteArray.from(new byte[1025]);
        assertFalse(container.canTake(1025));
        container.append(key, value);
    }

    @Test
    public void testGet() {
        Container container = Containers.newBufferedMemoryContainer(10);
        ByteArray key1 = ByteArray.from(new byte[] { 2, 2, 22 });
        ByteArray value1 = ByteArray.from(new byte[] { 1 });
        ByteArray key2 = ByteArray.from(new byte[] { 4, 4, 44 });
        ByteArray value2 = ByteArray.from(new byte[] { 2 });
        ByteArray key3 = ByteArray.from(new byte[] { 8, 8, 88 });
        ByteArray value3 = ByteArray.from(new byte[] { 3 });

        container.append(key1, value1);
        container.append(key2, value2);
        container.append(key3, value3);

        assertTrue(container.containsKey(key1));
        assertTrue(container.containsKey(key2));
        assertTrue(container.containsKey(key3));

        Optional<ByteArray> op1 = container.get(key1);
        Optional<ByteArray> op2 = container.get(key2);
        Optional<ByteArray> op3 = container.get(key3);

        assertTrue(op1.isPresent());
        assertTrue(op2.isPresent());
        assertTrue(op3.isPresent());

        assertEquals(value1, op1.get());
        assertEquals(value2, op2.get());
        assertEquals(value3, op3.get());
    }

    @Test
    public void testGetBadKey() {
        Container container = Containers.newBufferedMemoryContainer(10);
        ByteArray key1 = ByteArray.from(new byte[] { 2, 2, 22 });
        ByteArray value1 = ByteArray.from(new byte[] { 1 });
        ByteArray key2 = ByteArray.from(new byte[] { 4, 4, 44 });
        container.append(key1, value1);
        assertFalse(container.containsKey(key2));
        Optional<ByteArray> op1 = container.get(key2);
        assertFalse(op1.isPresent());
    }

}
