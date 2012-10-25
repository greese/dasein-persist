/**
 * Copyright (C) 1998-2011 enStratusNetworks LLC
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.persist;

import org.dasein.persist.annotations.Index;
import org.dasein.persist.annotations.IndexType;
import org.dasein.persist.annotations.Schema;
import org.dasein.util.CachedItem;

import java.util.Currency;

@Schema(value="2012-08-1", entity="persistent_object", mappers={TestMapper.class})
public class SchemaChange implements CachedItem, Comparable<SchemaChange> {
    private double amount;
    private Currency currency;
    @Index(type=IndexType.PRIMARY)
    private long keyField;
    @Index(type=IndexType.SECONDARY)
    private String name;
    private String funDescription;
    @Index(type=IndexType.SECONDARY)
    private IndexType indexType;
    @Index(type=IndexType.FOREIGN, identifies=OtherObject.class)
    private Long otherObject;
    private JSONMapped mapped;

    public SchemaChange() { }
    
    public boolean equals(Object ob) {
        if( ob == null ) {
            return false;
        }
        if( !(ob instanceof SchemaChange) ) {
            return false;
        }
        return (keyField == ((SchemaChange)ob).keyField);
    }
    
    public double getAmount() {
        return amount;
    }
    
    public Currency getCurrency() {
        return currency;
    }
    
    public long getKeyField() {
        return keyField;
    }
    
    public IndexType getIndexType() {
        return indexType;
    }
    
    public String getName() {
        return name;
    }
    
    public String getFunDescription() {
        return funDescription;
    }

    public JSONMapped getMapped() {
        return mapped;
    }

    public long getOtherObjectId() {
        return (otherObject == null ? -1L : otherObject.longValue());
    }
    
    private transient volatile boolean valid = true;
    
    public void invalidate() {
        valid = false;
    }
    
    @Override
    public boolean isValidForCache() {
        return valid;
    }
    
    public String toString() { 
        return ("[#" + keyField + " - " + getName() + "]");
    }

    @Override
    public int compareTo(SchemaChange other) {
        return getName().compareTo(other.getName());
    }
}
