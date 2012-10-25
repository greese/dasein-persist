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
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.dasein.persist.annotations.IndexType;
import org.dasein.persist.jdbc.AutomatedSql.Operator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RiakTestCase extends TestCase {
    static private final Logger wire = Logger.getLogger("org.dasein.persist.wire.riak");
    private PersistentCache<PersistentObject> cache;
    
    private Logger logger;
    private TreeSet<PersistentObject> testMatches;
   
    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        cache = (PersistentCache<PersistentObject>)PersistentCache.getCache(PersistentObject.class);
        logger = Logger.getLogger("org.dasein.persist.test." + getName());
        
        wire.debug("********** " + getName() + "*********");
        logger.debug("BEGIN: " + getName());
        long id = 0L;
        if( !getName().equals("testCreate") ) {
            HashMap<String,Object> state = new HashMap<String,Object>();
            String name = "Get Name";
            String description = "Get Description";
            JSONMapped mapped = JSONMapped.getInstance(System.currentTimeMillis(), name);

            id++;
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            state.put("indexType", IndexType.FOREIGN);
            state.put("currency", Currency.getInstance("USD"));
            state.put("amount", 24.56);
            state.put("otherObject", 151L);
            state.put("mapped", mapped);
            state.put("indexA", "a");
            state.put("indexB", "unknown");
            state.put("indexC", "unknown");
            Transaction xaction = Transaction.getInstance();
            
            try {
                PersistentObject item = cache.create(xaction, state);
                
                xaction.commit();
                assertNotNull("No object was created", item);
                assertEquals("ID does not match", id, item.getKeyField());
                assertEquals("Name does not match", name, item.getName());
                assertEquals("Description does not match", description, item.getDescription());
                assertEquals("Mapped values do not match", mapped, item.getMapped());
            }
            finally {
                xaction.rollback();
            }
        }
        if( getName().equals("testFindMulti") ) {
            testMatches = new TreeSet<PersistentObject>();
            HashMap<String,Object> state = new HashMap<String,Object>();
            String name = "Multi Test";
            String description = "A multi test object";

            id++;
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            state.put("indexType", IndexType.SECONDARY);
            state.put("currency", Currency.getInstance("USD"));
            state.put("amount", 78.90);
            state.put("indexA", "a");
            state.put("indexB", "b");
            state.put("indexC", "unknown");

            Transaction xaction = Transaction.getInstance();

            try {
                PersistentObject item = cache.create(xaction, state);

                testMatches.add(item);
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }
        }
        if( getName().equals("testFindMultiIncomplete") ) {
            testMatches = new TreeSet<PersistentObject>();
            HashMap<String,Object> state = new HashMap<String,Object>();
            String name = "Multi Incomplete Test";
            String description = "A multi incomplete test object";

            id++;
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            state.put("indexType", IndexType.SECONDARY);
            state.put("currency", Currency.getInstance("USD"));
            state.put("amount", 78.90);
            state.put("indexA", "a");
            state.put("indexB", "b");
            state.put("indexC", "c");

            Transaction xaction = Transaction.getInstance();

            try {
                PersistentObject item = cache.create(xaction, state);

                testMatches.add(item);
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }
        }
        if( getName().equals("testFindLike") || getName().equals("testSort") ) {
            testMatches = new TreeSet<PersistentObject>();
            HashMap<String,Object> state = new HashMap<String,Object>();
            String name = "Second Test";
            String description = "A second test object";

            id++;
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            state.put("indexType", IndexType.FOREIGN);
            state.put("currency", Currency.getInstance("USD"));
            state.put("amount", 78.90);
            Transaction xaction = Transaction.getInstance();
            
            try {
                PersistentObject item = cache.create(xaction, state);
                
                testMatches.add(item);
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }

            state = new HashMap<String,Object>();
            name = "Third Test";
            description = "A third Test object";
            id++;
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            state.put("indexType", IndexType.FOREIGN);
            state.put("currency", Currency.getInstance("USD"));
            state.put("amount", 78.90);
            xaction = Transaction.getInstance();
            
            try {
                PersistentObject item = cache.create(xaction, state);
                
                testMatches.add(item);
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }

            state = new HashMap<String,Object>();
            name = "Fourth Test";
            description = "Test number four";
            id++;
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            state.put("indexType", IndexType.PRIMARY);
            state.put("currency", Currency.getInstance("NZD"));
            state.put("amount", 12.34);
            xaction = Transaction.getInstance();
            
            try {
                PersistentObject item = cache.create(xaction, state);
                
                testMatches.add(item);
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }
        }
    }
    
    @After
    @Override
    public void tearDown() {
        try {
            if( cache != null ) {
                try {
                    for( PersistentObject item : cache.list() ) {
                        Transaction xaction = Transaction.getInstance();
                        
                        try {
                            cache.remove(xaction, item);
                            item.invalidate();
                            xaction.commit();
                        }
                        finally {
                            xaction.rollback();
                        }
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
        }
        finally {
            logger.debug("END: " + getName());            
        }
    }
    
    @Test
    public void testCount() throws PersistenceException {
        long count = cache.count();
        
        logger.debug("Count: " + count);
        assertEquals("Count does not match expected single row", 1, count);
    }
    
    @Test
    public void testCountEmpty() throws PersistenceException {
        long count = cache.count(new SearchTerm("name", "No such name"));
        
        logger.debug("Count: " + count);
        assertEquals("Count does not match expected non-match", 0, count);
    }
    
    @Test
    public void testCountMatch() throws PersistenceException {
        long count = cache.count(new SearchTerm("name", "Get Name"));
        
        logger.debug("Count: " + count);
        assertEquals("Count does not match expected match", 1, count);
    }
    
    @Test
    public void testCreate() throws PersistenceException {
        HashMap<String,Object> state = new HashMap<String,Object>();
        long id = System.currentTimeMillis();
        String name = "Create - " + id;
        String description = "Creating persistent object " + id;
        
        state.put("keyField", id);
        state.put("name", name);
        state.put("description", description);
        
        state.put("indexType", IndexType.FOREIGN);
        state.put("currency", Currency.getInstance("GBP"));
        state.put("amount", 56.78);
        
        Transaction xaction = Transaction.getInstance();
        
        try {
            PersistentObject item = cache.create(xaction, state);
            
            xaction.commit();
            assertNotNull("No object was created", item);
            assertEquals("ID does not match", id, item.getKeyField());
            assertEquals("Name does not match", name, item.getName());
            assertEquals("Description does not match", description, item.getDescription());
        }
        finally {
            xaction.rollback();
        }
    }
    
    @Test
    public void testGet() throws PersistenceException {
        PersistentObject item = cache.get(1L);
        
        assertNotNull("No object exists", item);
        assertEquals("ID does not match", 1L, item.getKeyField());
        assertEquals("Name does not match", "Get Name", item.getName());
        assertEquals("Description does not match", "Get Description", item.getDescription());
        assertEquals("Index type does not match.", IndexType.FOREIGN, item.getIndexType());
        assertEquals("Currency does not match", "USD", item.getCurrency().getCurrencyCode());
        System.out.println(item.getMapped());
    }
    
    @Test
    public void testFindEmpty() throws PersistenceException {
        int count = 0;

        for( @SuppressWarnings("unused") PersistentObject item : cache.find(new SearchTerm("name", "No such value")) ) {
            count++;
        }
        assertEquals("Matching rows unexpectedly found", 0, count);        
    }
    
    @Test
    public void testFindNamed() throws PersistenceException {
        int count = 0;

        for( PersistentObject item : cache.find(new SearchTerm("name", "Get Name")) ) {
            assertEquals("Item name does not match search", "Get Name", item.getName());
            count++;
        }
        assertEquals("Matching rows unexpectedly found", 1, count);        
    }

    @Test
    public void testFindMulti() throws PersistenceException {
        SearchTerm[] terms = new SearchTerm[] { new SearchTerm("indexA", "a"), new SearchTerm("indexB", "b") };
        int count = 0;

        for( PersistentObject item : cache.find(terms) ) {
            assertEquals("Index A does not match", "a", item.getIndexA());
            assertEquals("Index B does not match", "b", item.getIndexB());
            count++;
        }
        assertTrue("No matches found", count > 0);
    }

    @Test
    public void testFindMultiIncomplete() throws PersistenceException {
        SearchTerm[] terms = new SearchTerm[] { new SearchTerm("indexA", "a"), new SearchTerm("indexB", "b"), new SearchTerm("indexC", "c") };
        int count = 0;

        for( PersistentObject item : cache.find(terms) ) {
            assertEquals("Index A does not match", "a", item.getIndexA());
            assertEquals("Index B does not match", "b", item.getIndexB());
            assertEquals("Index C does not match", "c", item.getIndexC());
            count++;
        }
        assertTrue("No matches found", count > 0);
    }

    @Test
    public void testFindLike() throws PersistenceException {
        int count = 0;

        for( PersistentObject item : cache.find(new SearchTerm("description", Operator.LIKE, "test")) ) {
            logger.debug("Item: " + item);
            count++;
        }
        assertEquals("Matching rows unexpectedly found", 3, count);        
    }
    
    @Test
    public void testList() throws PersistenceException {
        int count = 0;

        for( PersistentObject item : cache.list() ) {
            logger.debug("Item: " + item);
            count++;
        }
        assertEquals("List count did not match expected", 1, count);
    }
    
    @Test
    public void testRemove() throws PersistenceException {
        PersistentObject item = cache.get(1L);
        
        assertNotNull("No object exists, unable to test remove", item);
        Transaction xaction = Transaction.getInstance();
        
        try {
            cache.remove(xaction, item);
            xaction.commit();
        }
        finally  {
            xaction.rollback();
        }
        item = cache.get(1L);
        assertNull("Item is still in cache", item);
    }
    
    @Test
    public void testSort() throws PersistenceException {
        TreeSet<PersistentObject> matches = new TreeSet<PersistentObject>();

        for( PersistentObject item : cache.find(new SearchTerm[] { new SearchTerm("description", Operator.LIKE, "test") }, null, false, "name") ) {
            logger.debug("Item: " + item);
            matches.add(item);
        }
        assertEquals("Result sets do not match", matches.size(), testMatches.size());
        Iterator<PersistentObject> first = matches.iterator();
        Iterator<PersistentObject> second = testMatches.iterator();
        
        while( first.hasNext() ) {
            PersistentObject fo = first.next();
            PersistentObject so = second.next();
            
            assertEquals("Result sets do not match", fo, so);
        }
    }
    
    @Test
    public void testUpdate() throws PersistenceException {
        PersistentObject item = cache.get(1L);
        
        assertNotNull("No object exists, unable to test update", item);
        Transaction xaction = Transaction.getInstance();
        
        try {
            HashMap<String,Object> state = new HashMap<String,Object>();
            
            state.put("keyField", 1L);
            state.put("name", "New Name");
            state.put("description", "New Description");
            cache.update(xaction, item, state);
            item.invalidate();
            xaction.commit();
        }
        finally  {
            xaction.rollback();
        }
        item = cache.get(1L);
        assertEquals("Name equals an unexpected value: " + item.getName(), "New Name", item.getName());
    }

    @Test
    public void testConversion() throws PersistenceException {
        PersistentObject item = cache.get(1L);

        assertNotNull("No object exists, unable to test conversion", item);

        PersistentCache<SchemaChange> changeCache = (PersistentCache<SchemaChange>)PersistentCache.getCache(SchemaChange.class);

        SchemaChange sameObject = changeCache.get(1L);

        assertNotNull("Unable to find converted object", sameObject);

        assertEquals("Description value was not properly converted", item.getDescription(), sameObject.getFunDescription());
    }
}
