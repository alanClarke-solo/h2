// HierarchicalCacheAspect.java (AOP support)
package com.h2.spring.cache.aspect;

import ac.h2.SearchParameter;
import com.example.cache.annotation.HierarchicalCacheable;
import com.example.cache.service.HierarchicalCacheService;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Aspect
@Component
public class HierarchicalCacheAspect {

    private final HierarchicalCacheService cacheService;
    private final ExpressionParser parser = new SpelExpressionParser();

    public HierarchicalCacheAspect(HierarchicalCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Around("@annotation(hierarchicalCacheable)")
    public Object aroundCacheableMethod(ProceedingJoinPoint joinPoint, 
                                       HierarchicalCacheable hierarchicalCacheable) throws Throwable {
        
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        
        // Generate cache key
        String cacheKey = generateCacheKey(hierarchicalCacheable, method, args);
        
        // Generate search parameters
        List<SearchParameter> parameters = generateSearchParameters(hierarchicalCacheable, method, args);
        
        // Try to get from cache
        Object cachedResult = cacheService.get(cacheKey, method.getReturnType(), hierarchicalCacheable.cacheLevel())
                .orElse(null);
                
        if (cachedResult != null) {
            return cachedResult;
        }
        
        // Execute method and cache result
        Object result = joinPoint.proceed();
        
        if (result != null) {
            Duration ttl = hierarchicalCacheable.ttlMinutes() > 0 ? 
                Duration.ofMinutes(hierarchicalCacheable.ttlMinutes()) : null;
                
            cacheService.put(cacheKey, null, parameters, result, 
                hierarchicalCacheable.cacheLevel(), ttl);
        }
        
        return result;
    }

    private String generateCacheKey(HierarchicalCacheable annotation, Method method, Object[] args) {
        if (!annotation.key().isEmpty()) {
            return evaluateSpelExpression(annotation.key(), method, args);
        }
        
        StringBuilder keyBuilder = new StringBuilder(method.getDeclaringClass().getSimpleName())
                .append(".")
                .append(method.getName());
                
        if (args.length > 0) {
            keyBuilder.append("(");
            for (int i = 0; i < args.length; i++) {
                if (i > 0) keyBuilder.append(",");
                keyBuilder.append(args[i] != null ? args[i].toString() : "null");
            }
            keyBuilder.append(")");
        }
        
        return keyBuilder.toString();
    }

    private List<SearchParameter> generateSearchParameters(HierarchicalCacheable annotation, 
                                                          Method method, Object[] args) {
        List<SearchParameter> parameters = new ArrayList<>();
        
        String[] paramNames = annotation.parameters();
        int[] levels = annotation.levels();
        
        for (int i = 0; i < paramNames.length && i < levels.length; i++) {
            String paramValue = evaluateSpelExpression(paramNames[i], method, args);
            parameters.add(new SearchParameter(paramNames[i], paramValue, levels[i]));
        }
        
        return parameters;
    }

    private String evaluateSpelExpression(String expression, Method method, Object[] args) {
        EvaluationContext context = new StandardEvaluationContext();
        
        // Add method parameters to context
        String[] paramNames = getParameterNames(method);
        for (int i = 0; i < args.length && i < paramNames.length; i++) {
            context.setVariable(paramNames[i], args[i]);
        }
        
        try {
            Object result = parser.parseExpression(expression).getValue(context);
            return result != null ? result.toString() : "";
        } catch (Exception e) {
            return expression; // Fallback to literal string
        }
    }

    private String[] getParameterNames(Method method) {
        // In a real implementation, you might use reflection or parameter name discovery
        // For simplicity, using generic names
        String[] names = new String[method.getParameterCount()];
        for (int i = 0; i < names.length; i++) {
            names[i] = "arg" + i;
        }
        return names;
    }
}
