package ac.h2;// SearchParameterTest.java
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SearchParameterTest {

    @Test
    void testSearchParameterCreation() {
        // Arrange & Act
        SearchParameter param = new SearchParameter("region", "US", 0);
        
        // Assert
        assertEquals("region", param.getName());
        assertEquals("US", param.getValue());
        assertEquals(0, param.getLevel());
    }

    @Test
    void testToKey() {
        // Arrange
        SearchParameter param = new SearchParameter("category", "electronics", 1);
        
        // Act
        String key = param.toKey();
        
        // Assert
        assertEquals("L1:category=electronics", key);
    }

    @Test
    void testEquals() {
        // Arrange
        SearchParameter param1 = new SearchParameter("brand", "apple", 2);
        SearchParameter param2 = new SearchParameter("brand", "apple", 2);
        SearchParameter param3 = new SearchParameter("brand", "samsung", 2);
        SearchParameter param4 = new SearchParameter("brand", "apple", 3);
        
        // Assert
        assertEquals(param1, param2);
        assertNotEquals(param1, param3);
        assertNotEquals(param1, param4);
        assertNotEquals(param1, null);
        assertNotEquals(param1, "string");
    }

    @Test
    void testHashCode() {
        // Arrange
        SearchParameter param1 = new SearchParameter("brand", "apple", 2);
        SearchParameter param2 = new SearchParameter("brand", "apple", 2);
        
        // Assert
        assertEquals(param1.hashCode(), param2.hashCode());
    }

    @Test
    void testToString() {
        // Arrange
        SearchParameter param = new SearchParameter("product", "cellphone", 3);
        
        // Act
        String toString = param.toString();
        
        // Assert
        assertEquals("L3:product=cellphone", toString);
    }
}
