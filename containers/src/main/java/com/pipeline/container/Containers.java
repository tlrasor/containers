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

import com.google.common.base.Supplier;
import com.pipeline.commons.util.file.TempFileManager;
import com.pipeline.conditions.Condition;

/**
 * @author Travis Rasor
 */
public class Containers {

    public static Container newBufferedMemoryContainer(int size) {
        Condition.that(size > 0).isTrue();
        return new BufferedMemoryContainer(size);
    }

    public static Container newFileContainer(int size) {
        Condition.that(size > 0).isTrue();
        return FileContainer.fromFile(TempFileManager.Instance.createNewTempFile(), size);
    }

    public static MapContainer newMapContainer(int size) {
        Condition.that(size > 0).isTrue();
        return new MapContainer(size);
    }

    public static Supplier<Container> newInMemoryContainerSupplier(final int size) {
        return new Supplier<Container>() {

            public Container get() {
                return newBufferedMemoryContainer(size);
            }
        };
    }

    public static Supplier<Container> newFileContainerSupplier(final int size) {
        return new Supplier<Container>() {

            public Container get() {
                return newFileContainer(size);
            }
        };
    }

    public static Supplier<Container> newMapContainerSupplier(final int size) {
        return new Supplier<Container>() {

            public Container get() {
                return newMapContainer(size);
            }
        };
    }
}
