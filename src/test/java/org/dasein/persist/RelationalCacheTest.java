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

import junit.framework.TestCase;

import org.junit.Test;

public class RelationalCacheTest extends TestCase {
    @Test
    public void testNothing() { }
    /*
    private PersistentCache<PersistentObject> cache  = null;
    private MysqlConnectionPoolDataSource     testDs = null;
    
    private void modifyState() {
        try {
            Connection connection = testDs.getConnection();
            Statement stmt = connection.createStatement();
            
            stmt.executeUpdate("UPDATE persistent_object SET name ='New Name', description = 'New Description'");
            stmt.close();
            connection.close();
        }
        catch( Throwable e ) {
            fail("Database error setting up state: " + e.getMessage());
        }
    }
    
    @Before
    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        System.setProperty("org.osjava.sj.jndi.shared", "true");
        System.setProperty("java.naming.factory.initial", "org.osjava.sj.memory.MemoryContextFactory");
        
        testDs = new MysqlConnectionPoolDataSource();
        testDs.setUser("dasein");
        testDs.setPassword("daseintest");
        testDs.setServerName("204.236.194.84");
        testDs.setPort(3306);
        testDs.setDatabaseName("dasein");
        testDs.setAutoReconnect(true);
        testDs.setCharacterEncoding("UTF-8");
        testDs.setUseUnicode(true);

        InitialContext ic = new InitialContext();
        
        ic.rebind("java:comp/env/jdbc/dasein", testDs); 
        
        cache = (PersistentCache<PersistentObject>)PersistentCache.getCache(PersistentObject.class, "keyField");

        if( !getName().equals("testCreate") ) {
            HashMap<String,Object> state = new HashMap<String,Object>();
            long id = 1L;
            String name = "Get Name";
            String description = "Get Description";
            
            state.put("keyField", id);
            state.put("name", name);
            state.put("description", description);
            
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
    }
    
    @After
    @Override
    public void tearDown() {
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
        if( testDs != null ) {
            try {
                Connection connection = testDs.getConnection();
                Statement stmt = connection.createStatement();
                
                stmt.executeUpdate("DELETE FROM persistent_object");
                stmt.close();
                connection.close();
            }
            catch( Throwable ignore ) {
                // ignore
            }
        }
    }
    
    @Test
    public void testValidCache() throws PersistenceException {
        PersistentObject item = cache.get(1L);
                
        modifyState();
        
        PersistentObject copy = cache.get(1L);
        
        assertEquals("New item name is not cached value", item.getName(), copy.getName());
        assertEquals("New item description is not cached value", item.getDescription(), copy.getDescription());
    }
    
    @Test
    public void testInvalidCache() throws PersistenceException {
        PersistentObject item = cache.get(1L);
                
        modifyState();
        item.invalidate();
        
        PersistentObject copy = cache.get(1L);
        
        assertTrue("New item name is not cached value", !item.getName().equals(copy.getName()));
        assertTrue("New item description is not cached value", !item.getDescription().equals(copy.getDescription()));
    }
    
    @Test
    public void testCount() throws PersistenceException {
        assertEquals("Count does not match expected single row", 1, cache.count());
    }
    
    @Test
    public void testCountEmpty() throws PersistenceException {
        assertEquals("Count does not match expected non-match", 0, cache.count(new SearchTerm("name", "No such name")));
    }
    
    @Test
    public void testCountMatch() throws PersistenceException {
        assertEquals("Count does not match expected match", 1, cache.count(new SearchTerm("name", "Get Name")));
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
    public void testList() throws PersistenceException {
        int count = 0;

        for( @SuppressWarnings("unused") PersistentObject item : cache.list() ) {
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
    */
}
