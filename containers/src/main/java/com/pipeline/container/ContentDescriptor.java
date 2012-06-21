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

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.pipeline.conditions.Condition;

/**
 * Simple pojo to describe where in a linear container this piece of content may
 * be found
 * 
 * @author Travis Rasor
 */
public class ContentDescriptor {
    final int position;
    final int length;

    public ContentDescriptor(int position, int offset) {
        Condition.that(position >= 0 && position <= offset);
        this.position = position;
        this.length = offset;
    }

    public int getPosition() {
        return position;
    }

    public int getLength() {
        return length;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(711, 2231).append(position).append(length).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof ContentDescriptor)) return false;
        ContentDescriptor other = (ContentDescriptor) obj;
        if (length != other.length) return false;
        if (position != other.position) return false;
        return true;
    }

    @Override
    public String toString() {
        return "position: " + position + ", offset: " + length;
    }

}
