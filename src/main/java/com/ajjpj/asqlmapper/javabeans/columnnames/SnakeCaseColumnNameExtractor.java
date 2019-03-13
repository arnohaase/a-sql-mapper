package com.ajjpj.asqlmapper.mapper.beans.javatypes;


public class SnakeCaseColumnNameExtractor extends AnnotationBasedColumnNameExtractor {
    @Override
    protected String propertyNameToColumnName (Class<?> beanType, String propertyName) {
        final StringBuilder result = new StringBuilder(propertyName.length());

        boolean wasLowerCase = false;
        for(char ch: propertyName.toCharArray()) {
            final boolean isLowerCase = Character.isLowerCase(ch);
            if(isLowerCase) {
                result.append(ch);
            }
            else {
                if(wasLowerCase) {
                    result.append('_');
                }
                result.append(Character.toLowerCase(ch));
            }
            wasLowerCase = isLowerCase;
        }
        return result.toString();
    }
}
