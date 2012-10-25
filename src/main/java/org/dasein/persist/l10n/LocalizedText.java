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

package org.dasein.persist.l10n;

import java.io.Serializable;
import java.text.Collator;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;

import org.dasein.persist.PersistenceException;
import org.dasein.persist.PersistentFactory;
import org.dasein.persist.SearchTerm;
import org.dasein.persist.Transaction;

public class LocalizedText implements Comparable<LocalizedText>, Serializable {
    private static final long serialVersionUID = -6025749948417757531L;
    
    static private PersistentFactory<LocalizedText> factory = new PersistentFactory<LocalizedText>(LocalizedText.class, "localizationCode");

    static public void addTranslation(String textGroup, String textCode, Locale forLocale, String translation) throws PersistenceException {
        String localizationCode = (textGroup + ":" + textCode + ":" + forLocale.getLanguage() + ":" + forLocale.getCountry() + ":" + forLocale.getVariant()).toLowerCase();
        HashMap<String,Object> state = new HashMap<String,Object>();
        
        state.put("country", forLocale.getCountry());
        state.put("language", forLocale.getLanguage());
        state.put("localizationCode", localizationCode);
        state.put("textCode", textCode);
        state.put("textGroup", textGroup);
        state.put("textMessage", translation);
        state.put("variant", forLocale.getVariant());

        Transaction xaction  = Transaction.getInstance();
        
        try {
            synchronized( factory ) {
                LocalizedText text = factory.get("localizationCode", localizationCode);
                
                if( text == null ) {
                    factory.create(xaction, state);
                }
                else {
                    factory.update(xaction, text, state);
                }
            }
            xaction.commit();
        }
        finally {
            xaction.rollback();
        }
    }
    
    static public Collection<LocalizedText> getTranslations(String textGroup, String textCode) throws PersistenceException {
        SearchTerm[] terms = new SearchTerm[2];
        
        terms[0] = new SearchTerm("textGroup", textGroup);
        terms[1] = new SearchTerm("textCode", textCode);
        return factory.find(terms, false, "language", "country", "variant");
    }
    
    private String country          = null;
    private String language         = null;
    private String localizationCode = null;
    private String textCode         = null;
    private String textGroup        = null;
    private String textMessage      = null;
    private String variant          = null;
    
    public LocalizedText() { }

    public int compareTo(LocalizedText other) {
        if( other == null ) {
            return -1;
        }
        if( other == this ) {
            return 0;
        }
        if( !getLanguage().equals(other.getLanguage()) ) {
            return getLanguage().compareTo(other.getLanguage());
        }
        Locale sortingLocale;
        
        if( (getCountry() == null && other.getCountry() != null) || (getCountry() != null && other.getCountry() == null) || (getCountry() != null && !getCountry().equals(other.getCountry())) ) {
            sortingLocale = new Locale(getLanguage());
        }
        else {
            sortingLocale = new Locale(getLanguage(), getCountry());
        }
        return Collator.getInstance(sortingLocale).compare(getTextMessage(), other.getTextMessage());
    }
    
    public boolean equals(Object ob) {
        if( ob == null ) {
            return false;
        }
        if( ob == this ) {
            return true;
        }
        if( !getClass().getName().equals(ob.getClass().getName()) ) {
            return false;
        }
        return getLocalizationCode().equals(((LocalizedText)ob).getLocalizationCode());
    }
    
    public String getCountry() {
        return country;
    }

    public String getLanguage() {
        return language;
    }
    
    public Locale getLocale() {
        if( variant == null ) {
            if( country == null ) {
                return new Locale(language);
            }
            return new Locale(language, country);
        }
        return new Locale(language, country, variant);
    }
    
    public String getLocalizationCode() {
        return localizationCode;
    }

    public String getTextCode() {
        return textCode;
    }
    
    public String getTextGroup() {
        return textGroup;
    }
    
    public String getTextMessage() {
        return textMessage;
    }
    
    public String getVariant() {
        return variant;
    }
    
    private transient volatile int hashCode = -1;
    
    public int hashCode() {
        if( hashCode == -1 ) {
            hashCode = getLocalizationCode().hashCode();
        }
        return hashCode;
    }
    
    public String toString() {
        return textMessage;
    }
}
