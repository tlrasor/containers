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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.pipeline.commons.util.ByteArray;
import com.pipeline.conditions.Condition;

/**
 * @author Travis Rasor
 */
public class ByteBufferContainer implements Container {

    final ByteBuffer buffer;
    final Map<ByteArray, ContentDescriptor> offsetTable;
    final ContentReader reader;
    final ContentWriter writer;
    volatile int currentWritePosition;
    volatile int byteSize;
    volatile int size;

    public ByteBufferContainer(Supplier<ByteBuffer> bufferSupplier) {
        this.buffer = bufferSupplier.get();
        this.offsetTable = Maps.newConcurrentMap();
        this.reader = new ContentReader();
        this.writer = new ContentWriter();
    }

    public Optional<ByteArray> get(ByteArray key) {
        ContentDescriptor contentDescriptor = offsetTable.get(key);
        if (contentDescriptor == null) {
            return Optional.absent();
        }
        ByteArray value = reader.readContent(contentDescriptor);
        return Optional.of(value);
    }

    public void append(ByteArray key, ByteArray data) {
        Condition.that(offsetTable.containsKey(key)).isFalse().andThat(canTake(data.getArray().length)).isTrue();
        ContentDescriptor descriptor = new ContentDescriptor(currentWritePosition, data.getArray().length);
        offsetTable.put(key, descriptor);
        writer.writeContent(descriptor, data);
        currentWritePosition += data.getArray().length;
        size++;
        byteSize += data.getArray().length;
    }

    public boolean containsKey(ByteArray key) {
        return offsetTable.containsKey(key);
    }

    @Override
    public Set<ByteArray> keySet() {
        return offsetTable.keySet();
    }

    public int size() {
        return size;
    }

    public int sizeInBytes() {
        return byteSize;
    }

    public int maximumSizeInBytes() {
        return buffer.capacity();
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean canTake(int size) {
        return (maximumSizeInBytes() - byteSize >= size) ? true : false;
    }

    class ContentReader {

        ByteArray readContent(ContentDescriptor contentDescriptor) {
            byte[] data = new byte[contentDescriptor.getLength()];
            buffer.position(contentDescriptor.getPosition());
            buffer.get(data);
            return ByteArray.from(data);
        }
    }

    class ContentWriter {
        void writeContent(ContentDescriptor contentDescriptor, ByteArray content) {
            buffer.position(contentDescriptor.getPosition());
            buffer.put(content.getArray());
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((buffer == null) ? 0 : buffer.hashCode());
        result = prime * result + byteSize;
        result = prime * result + currentWritePosition;
        result = prime * result + ((offsetTable == null) ? 0 : offsetTable.hashCode());
        result = prime * result + ((reader == null) ? 0 : reader.hashCode());
        result = prime * result + size;
        result = prime * result + ((writer == null) ? 0 : writer.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof ByteBufferContainer)) return false;
        ByteBufferContainer other = (ByteBufferContainer) obj;
        if (buffer == null) {
            if (other.buffer != null) return false;
        } else if (!buffer.equals(other.buffer)) return false;
        if (byteSize != other.byteSize) return false;
        if (currentWritePosition != other.currentWritePosition) return false;
        if (offsetTable == null) {
            if (other.offsetTable != null) return false;
        } else if (!offsetTable.equals(other.offsetTable)) return false;
        if (reader == null) {
            if (other.reader != null) return false;
        } else if (!reader.equals(other.reader)) return false;
        if (size != other.size) return false;
        if (writer == null) {
            if (other.writer != null) return false;
        } else if (!writer.equals(other.writer)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "ByteBufferContainer [buffer=" + buffer + ", offsetTable=" + offsetTable + ", reader=" + reader
                + ", writer=" + writer + ", currentWritePosition=" + currentWritePosition + ", byteSize=" + byteSize
                + ", size=" + size + "]";
    }
}
