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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.pipeline.conditions.Condition;

/**
 * @author Travis Rasor
 */
public class FileContainer extends ByteBufferContainer implements Closeable {

    public static FileContainer fromFile(File file, int size) {
        try {
            Condition.that(file).isNotNull().andThat(file.isFile());
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            return new FileContainer(raf, size);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    final RandomAccessFile file;

    FileContainer(RandomAccessFile file, int size) {
        super(initBuffer(file, size));
        this.file = file;
    }

    private static Supplier<ByteBuffer> initBuffer(RandomAccessFile file, int size) {
        try {

            ByteBuffer buffer = file.getChannel().map(MapMode.READ_WRITE, 0, size);
            return Suppliers.ofInstance(buffer);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void close() throws IOException {
        file.close();
    }

}
