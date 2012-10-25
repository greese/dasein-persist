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

import java.util.Currency;

import org.dasein.persist.annotations.Index;
import org.dasein.persist.annotations.IndexType;
import org.dasein.persist.annotations.Schema;
import org.dasein.util.CachedItem;

@Schema("2012-08")
public class PersistentObject implements CachedItem, Comparable<PersistentObject> {
    private double amount;
    private Currency currency;
    @Index(type=IndexType.PRIMARY)
    private long keyField;
    @Index(type=IndexType.SECONDARY)
    private String name;
    private String description;
    @Index(type=IndexType.SECONDARY)
    private IndexType indexType;
    @Index(type=IndexType.FOREIGN, identifies=OtherObject.class)
    private Long otherObject;
    private JSONMapped mapped;

    @Index( type=IndexType.SECONDARY, cascade = true, multi={"indexB"})
    private String indexA;
    private String indexB;
    private String indexC;

    public PersistentObject() { }
    
    public boolean equals(Object ob) {
        if( ob == null ) {
            return false;
        }
        if( !(ob instanceof PersistentObject) ) {
            return false;
        }
        return (keyField == ((PersistentObject)ob).keyField);
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
    
    public String getDescription() {
        return description;
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
    public int compareTo(PersistentObject other) {
        return getName().compareTo(other.getName());
    }

    public String getIndexA() {
        return indexA;
    }

    public String getIndexB() {
        return indexB;
    }

    public String getIndexC() {
        return indexC;
    }
}
