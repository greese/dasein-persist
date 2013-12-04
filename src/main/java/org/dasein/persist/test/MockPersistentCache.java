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

package org.dasein.persist.test;

import org.dasein.persist.Memento;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.PersistentCache;
import org.dasein.persist.SearchTerm;
import org.dasein.persist.Transaction;
import org.dasein.util.CachedItem;
import org.dasein.util.JiteratorFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Mock implementation of a persistent cache for users of Dasein Persistence to run tests against without
 * having to hook up to a database.
 * <p>Created by George Reese: 8/22/12 3:57 PM</p>
 * @author George Reese
 * @version 2012.08.2
 * @since 2012.08.2
 */
public class MockPersistentCache<T extends CachedItem> extends PersistentCache<T> {
    private final HashMap<Object,T> cache = new HashMap<Object, T>();

    public MockPersistentCache() { }

    @Override
    public T create(Transaction xaction, Map<String, Object> state) throws PersistenceException {
        synchronized( cache ) {
            Object key = state.get(getPrimaryKeyField());

            if( key == null ) {
                throw new PersistenceException("Expected a value for the primary key field " + getPrimaryKeyField());
            }
            if( cache.containsKey(key) ) {
                throw new PersistenceException("Duplicate key: " + key);
            }
            T item = toTargetFromMap(getSchemaVersion(), state);

            cache.put(key, item);
            return item;
        }
    }

    @Override
    public @Nonnull Collection<T> find(@Nonnull SearchTerm[] terms, @Nullable JiteratorFilter<T> filter, final @Nullable Boolean orderDesc, final @Nullable String... orderFields) throws PersistenceException {
        Collection<T> matches = new ArrayList<T>();

        for( T item : list() ) {
            boolean ok = true;

            for( SearchTerm term : terms ) {
                if( !matches(item, term) ) {
                    ok = false;
                    break;
                }
            }
            if( ok ) {
                matches.add(item);
            }
        }
        if( orderFields != null && orderFields.length > 0 ) {
            TreeSet<T> ordered = new TreeSet<T>(new Comparator<T>() {
                @Override
                public int compare(T first, T second) {
                    int result = 0;

                    for( String field : orderFields ) {
                        Object v1 = getValue(first, field);
                        Object v2 = getValue(second, field);

                        if( v1 != null && v2 == null ) {
                            result = -1;
                        }
                        else if( v1 == null && v2 != null ) {
                            result = 1;
                        }
                        else if( v1 != null ) {
                            if( v1 instanceof Comparable ) {
                                result = check((Comparable)v1, (Comparable)v2);
                            }
                        }
                        if( result != 0 ) {
                            break;
                        }
                    }
                    if( result != 0 ) {
                        return ((orderDesc == null || orderDesc) ? -result : result);
                    }
                    return 0;  //To change body of implemented methods use File | Settings | File Templates.
                }
            });

            ordered.addAll(matches);
            matches = ordered;
        }
        return matches;
    }

    private <R extends Comparable<R>> int check(R one, R two) {
        return one.compareTo(two);
    }
    @Override
    public T get(Object keyValue) throws PersistenceException {
        synchronized( cache ) {
            return cache.get(keyValue);
        }
    }

    @Override
    public String getSchema() throws PersistenceException {
        return "[no schema]";
    }

    @Override
    public Collection<T> list() throws PersistenceException {
        synchronized( cache ) {
            return cache.values();
        }
    }

    private boolean matches(@Nonnull T item, @Nonnull SearchTerm term) {
        Object value = getValue(item, term.getColumn());

        switch( term.getOperator() ) {
            case EQUALS: return (value != null && value.equals(term.getValue()));
            case NOT_EQUAL: return (value != null && !value.equals(term.getValue()));
            case NULL: return (value == null);
            case NOT_NULL: return (value != null);
            case LESS_THAN:
                if( value == null ) {
                    return false;
                }
                if( value instanceof Comparable ) {
                    return (check((Comparable)value, (Comparable)term.getValue()) < 0);
                }
                return (value.toString().compareTo(term.getValue() == null ? null : term.getValue().toString()) < 0);
            case LESS_THAN_OR_EQUAL_TO:
                if( value == null ) {
                    return false;
                }
                if( value instanceof Comparable ) {
                    return (check((Comparable)value, (Comparable)term.getValue()) < 1);
                }
                return (value.toString().compareTo(term.getValue() == null ? null : term.getValue().toString()) < 1);
            case GREATER_THAN:
                if( value == null ) {
                    return false;
                }
                if( value instanceof Comparable ) {
                    return (check((Comparable)value, (Comparable)term.getValue()) > -1);
                }
                return (value.toString().compareTo(term.getValue() == null ? null : term.getValue().toString()) > -1);
            case GREATER_THAN_OR_EQUAL_TO:
                if( value == null ) {
                    return false;
                }
                if( value instanceof Comparable ) {
                    return (check((Comparable)value, (Comparable)term.getValue()) > 0);
                }
                return (value.toString().compareTo(term.getValue() == null ? null : term.getValue().toString()) > 0);
            case LIKE:
                if( value == null ) {
                    return false;
                }
                String v = term.getValue().toString().replaceAll("%", "");

                return value.toString().contains(v);
        }
        return false;
    }

    @Override
    public void remove(Transaction xaction, T item) throws PersistenceException {
        Object key = getValue(item, getPrimaryKeyField());

        if( key == null ) {
            throw new PersistenceException("Key value was null");
        }
        synchronized( cache ) {
            if( !cache.containsKey(key) ) {
                throw new PersistenceException("No such key: " + key);
            }
            cache.remove(key);
        }
    }

    @Override
    public void remove(Transaction xaction, SearchTerm... terms) throws PersistenceException {
        synchronized( cache ) {
            for( T item : find(terms) ) {
                remove(xaction, item);
            }
        }
    }

    @Override
    public void update(Transaction xaction, T item, Map<String, Object> state) throws PersistenceException {
        synchronized( cache ) {
            Memento<T> memento = new Memento<T>(item);

            memento.save(state);
            state = memento.getState();

            Object key = state.get(getPrimaryKeyField());

            if( key == null ) {
                throw new PersistenceException("Expected a value for the primary key field " + getPrimaryKeyField());
            }
            if( !cache.containsKey(key) ) {
                throw new PersistenceException("No such key: " + key);
            }
            item = toTargetFromMap(getSchemaVersion(), state);
            cache.put(key, item);
        }
    }
    
    @Override
	public T get(SearchTerm... terms) throws PersistenceException {
		throw new PersistenceException("Not implemented");
	}
}
