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
import java.util.Collection;
import java.util.Locale;

import org.dasein.persist.PersistenceException;

public class LocalizationGroup implements Serializable {
    private static final long serialVersionUID = 2718148697277403210L;
    
    static public LocalizationGroup valueOf(String localizationCode) {
        String[] parts = localizationCode.split(":");
        
        return new LocalizationGroup(parts[0], parts[1]);
    }
    
    private String textGroup = null;
    private String textId    = null;
    
    public LocalizationGroup() { }
    
    public LocalizationGroup(String textGroup, String textId) {
        this.textGroup = textGroup;
        this.textId = textId;
    }
    
    private LocalizedText getBestTranslation(Locale locale, Collection<LocalizedText> options) {
        LocalizedText defaultTranslation = null;
        
        for( LocalizedText text : options ) {
            if( !locale.getLanguage().equals(text.getLanguage()) ) {
                continue;
            }
            if( text.getCountry() == null ) {
                if( locale.getCountry() == null ) {
                    return text;
                }
                if( defaultTranslation == null ) {
                    defaultTranslation = text;
                }
                continue;
            }
            if( locale.getCountry() == null ) {
                if( defaultTranslation == null ) {
                    defaultTranslation = text;
                }
                continue;
            }
            if( !locale.getCountry().equalsIgnoreCase(text.getCountry()) ) {
                if( defaultTranslation == null ) {
                    defaultTranslation = text;
                }
                continue;
            }
            if( text.getVariant() == null ) {
                if( locale.getVariant() == null ) {
                    return text;
                }
                if( defaultTranslation == null || defaultTranslation.getCountry() == null || !defaultTranslation.getCountry().equalsIgnoreCase(text.getCountry()) ) {
                    defaultTranslation = text;
                }
                continue;
            }
            if( locale.getVariant() == null ) {
                if( defaultTranslation == null ) {
                    defaultTranslation = text;
                }
                if( defaultTranslation.getCountry() == null || !defaultTranslation.getCountry().equalsIgnoreCase(text.getCountry()) ) {
                    defaultTranslation = text;
                }
                if( defaultTranslation.getVariant() == null || !defaultTranslation.getVariant().equalsIgnoreCase(text.getVariant()) ) {
                    defaultTranslation = text;
                }
                continue;
            }
            if( locale.getVariant().equalsIgnoreCase(text.getVariant()) ) {
                return text;
            }
            defaultTranslation = text;
        }
        return defaultTranslation;
    }
    
    private LocalizedText getExactTranslation(Locale locale, Collection<LocalizedText> options) {
        LocalizedText defaultTranslation = null;
        
        for( LocalizedText text : options ) {
            if( !locale.getLanguage().equals(text.getLanguage()) ) {
                continue;
            }
            if( text.getCountry() == null ) {
                if( locale.getCountry() == null ) {
                    return text;
                }
                continue;
            }
            if( locale.getCountry() == null ) {
                if( defaultTranslation == null || text.getVariant() == null ) {
                    defaultTranslation = text;
                }
                continue;
            }
            if( !locale.getCountry().equalsIgnoreCase(text.getCountry()) ) {
                continue;
            }
            if( text.getVariant() == null ) {
                if( locale.getVariant() == null ) {
                    return text;
                }
                continue;
            }
            if( locale.getVariant() == null ) {
                defaultTranslation = text;
                continue;
            }
            if( locale.getVariant().equalsIgnoreCase(text.getVariant()) ) {
                return text;
            }
        }
        return defaultTranslation;
    }
    
    public String getLocalizationCode() {
        return (textGroup + ":" + textId);
    }
    
    public String getTextGroup() {
        return textGroup;
    }
    
    public String getTextId() {
        return textId;
    }
    
    public void setTextGroup(String textGroup) {
        if( this.textGroup == null ) {
            this.textGroup = textGroup;
        }
    }
    
    public void setTextId(String textId) {
        if( this.textId == null ) {
            this.textId = textId;
        }
    }
    
    public LocalizedText getTranslation(Locale ... localePreferences) throws PersistenceException {
        if( localePreferences == null ) {
            localePreferences = new Locale[0];
        }
        Collection<LocalizedText> translations = LocalizedText.getTranslations(textGroup, textId);
        
        if( translations.size() < 1 ) {
            return null;
        }
        for( Locale locale : localePreferences ) {
            LocalizedText translation = getExactTranslation(locale, translations);
            
            if( translation != null ) {
                return translation;
            }
            translation = getBestTranslation(locale, translations);
            if( translation != null ) {
                return translation;
            }
        }
        LocalizedText translation = getExactTranslation(Locale.getDefault(), translations);
        
        if( translation != null ) {
            return translation;
        }
        translation = getBestTranslation(Locale.getDefault(), translations);
        if( translation != null ) {
            return translation;
        }
        return translations.iterator().next();
    }
    
    public String toString() {
        try {
            return getTranslation(Locale.getDefault()).getTextMessage();
        }
        catch( Throwable t ) {
            return getLocalizationCode();
        }
    }
}
