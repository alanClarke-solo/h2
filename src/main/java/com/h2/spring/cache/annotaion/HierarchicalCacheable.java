// HierarchicalCacheable.java (Custom annotation)
package com.h2.spring.cache.annotaion;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface HierarchicalCacheable {
    String value() default "default";
    String key() default "";
    String[] parameters() default {};
    int[] levels() default {};
    String cacheLevel() default "default";
    long ttlMinutes() default -1;
    boolean enableL1() default true;
    boolean enableL2() default true;
}
