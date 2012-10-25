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

import org.dasein.util.CachedItem;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

public final class Key implements Comparable<Key> {
    private String[] fields;
    private Class<? extends CachedItem> identifies;
    
    public Key(String ... fields) {
        this.fields = fields;
    }
    
    public Key(Class<? extends CachedItem> identifies, String field) {
        this.identifies = identifies;
        this.fields = new String[] { field };
    }
    
    public int compareTo(@Nullable Key other) {
        if( other == null ) {
            return -1;
        }
        if( other == this ) {
            return 0;
        }
        for( int i=0; i<fields.length; i++ ) {
            if( i >= other.fields.length ) {
                return -1;
            }
            int x = fields[i].compareTo(other.fields[i]);
            
            if( x != 0 ) {
                return x;
            }
        }
        if( fields.length == other.fields.length ) {
            return 0;
        }
        return 1;
    }
    
    public boolean equals(@Nullable Object ob) {
        if( ob == null ) {
            return false;
        }
        if( ob == this ) {
            return true;
        }
        if( !(ob instanceof Key) ) {
            return false;
        }
        Key k = (Key)ob;
        if( k.fields.length != fields.length ) {
            return false;
        }
        for( int i=0; i<fields.length; i++ ) {
            if( !fields[i].equals(k.fields[i]) ) {
                return false;
            }
        }
        return true;
    }
    
    public String[] getFields() {
        return fields;
    }
    
    public Class<? extends CachedItem> getIdentifies() {
        return identifies;
    }
    
    public boolean matches(SearchTerm ... terms) {
        if( terms == null || terms.length != fields.length ) {
            return false;
        }
        for( SearchTerm term : terms ) {
            boolean present = false;
            
            for( String field : fields ) {
                if( term.getColumn().equals(field) ) {
                    present = true;
                    break;
                }
            }
            if( !present ) {
                return false;
            }
        }
        for( String field : fields ) {
            boolean present = false;
            
            for( SearchTerm term : terms ) {
                if( term.getColumn().equals(field) ) {
                    present = true;
                    break;
                }                
            }
            if( !present ) {
                return false;
            }
        }
        return true;
    }
    
    public @Nonnull String toString() {
        return Arrays.toString(getFields());
    }
}
