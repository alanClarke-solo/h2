// HierarchicalCacheExample.java
import java.util.*;

public class HierarchicalCacheExample {
    
    public static void main(String[] args) {
        // Initialize cache service
        HierarchicalCacheService<String> cache = new HierarchicalCacheService<>(
            "redis://localhost:6379", 
            300000 // 5 minutes default TTL
        );

        cache.invalidateAll();

        // Sample data: iPhone specification
        String iphoneSpec = "iPhone 17s - 6.1-inch display, A18 chip, 128GB storage, 5G capable";
        
        // Create hierarchical parameters
        List<SearchParameter> iphoneParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product", "cellphone", 3)
        );

        // PUT operations
        System.out.println("=== PUT Operations ===");
        
        // Put with string key and parameters
        cache.put("iphone17s", iphoneParams, iphoneSpec);
        System.out.println("Cached iPhone with key 'iphone17s'");
        
        // Link the long ID to existing key
        cache.link("iphone17s", 2371L);
        System.out.println("Linked ID 2371 to 'iphone17s'");
        
        // PUT another item
        String samsungSpec = "Samsung Galaxy S25 - 6.2-inch display, Snapdragon 8 Gen 4, 256GB storage";
        List<SearchParameter> samsungParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "samsung", 2),
            new SearchParameter("product", "cellphone", 3)
        );
        cache.put("galaxy-s25", 2372L, samsungParams, samsungSpec);
        System.out.println("Cached Samsung Galaxy with key 'galaxy-s25' and ID 2372");

        // GET operations
        System.out.println("\n=== GET Operations ===");
        
        // Get by string key
        Optional<String> byKey = cache.get("iphone17s", String.class);
        System.out.println("Get by key 'iphone17s': " + byKey.orElse("Not found"));
        
        // Get by long ID
        Optional<String> byId = cache.get(2371L, String.class);
        System.out.println("Get by ID 2371: " + byId.orElse("Not found"));
        
        // Get by key + ID combination
        Optional<String> byKeyAndId = cache.get("iphone17s", 2371L, String.class);
        System.out.println("Get by key+ID: " + byKeyAndId.orElse("Not found"));
        
        // Get by hierarchical parameters
        System.out.println("\n=== Hierarchical Search ===");
        
        // Search by region only (should return both items)
        List<SearchParameter> regionOnly = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );
        List<String> regionResults = cache.get(regionOnly, String.class);
        System.out.println("Search by region=US: " + regionResults.size() + " results");
        regionResults.forEach(System.out::println);
        
        // Search by region and category
        List<SearchParameter> regionCategory = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        List<String> categoryResults = cache.get(regionCategory, String.class);
        System.out.println("\nSearch by region+category: " + categoryResults.size() + " results");
        
        // Search by brand only
        List<SearchParameter> brandOnly = Arrays.asList(
            new SearchParameter("brand", "apple", 2)
        );
        List<String> brandResults = cache.get(brandOnly, String.class);
        System.out.println("\nSearch by brand=apple: " + brandResults.size() + " results");
        brandResults.forEach(System.out::println);
        
        // Search with gaps (L0 + L3, skipping L1, L2)
        List<SearchParameter> gappedSearch = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("product", "cellphone", 3)
        );
        List<String> gappedResults = cache.get(gappedSearch, String.class);
        System.out.println("\nSearch with gaps (L0+L3): " + gappedResults.size() + " results");

        // GET OR COMPUTE operations
        System.out.println("\n=== GET OR COMPUTE Operations ===");
        
        String computed = cache.getOrCompute("new-key", String.class, () -> {
            System.out.println("Computing new value...");
            return "Computed value for new-key";
        });
        System.out.println("GetOrCompute result: " + computed);
        
        // LINK operations
        System.out.println("\n=== LINK Operations ===");
        
        // Add additional parameters to existing item
        List<SearchParameter> additionalParams = Arrays.asList(
            new SearchParameter("color", "black", 4),
            new SearchParameter("storage", "128GB", 5)
        );
        cache.link("iphone17s", additionalParams);
        System.out.println("Added additional parameters to iPhone");
        
        // Now search by the new parameters
        List<SearchParameter> colorSearch = Arrays.asList(
            new SearchParameter("color", "black", 4)
        );
        List<String> colorResults = cache.get(colorSearch, String.class);
        System.out.println("Search by color=black: " + colorResults.size() + " results");

        // STATISTICS
        System.out.println("\n=== Statistics ===");
        System.out.println(cache.getStatistics());

        // INVALIDATION
        System.out.println("\n=== Invalidation ===");
        cache.invalidate("galaxy-s25");
        System.out.println("Invalidated Samsung Galaxy");
        
        Optional<String> afterInvalidation = cache.get("galaxy-s25", String.class);
        System.out.println("Get Samsung after invalidation: " + afterInvalidation.orElse("Not found"));
        
        System.out.println("\nFinal statistics:");
        System.out.println(cache.getStatistics());
        
        // Cleanup
        cache.shutdown();
    }
}
