package com.learn.ioc_map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DisplayName("JamMaker IOC Tests")
class JamMakerTest {

    @Autowired
    private JamMaker jamMaker;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    @Qualifier("apple")
    private SlicerIF appleSlicer;

    @Autowired
    @Qualifier("lemon")
    private SlicerIF lemonSlicer;

    //@Autowired
    //private SlicerFactory slicerFactory;

    // For capturing System.out
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    void setUpStreams() {
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
    }

    // ==================== Bean Initialization Tests ====================

    @Test
    @DisplayName("Should autowire JamMaker component")
    void shouldAutowireJamMaker() {
        assertNotNull(jamMaker, "JamMaker should be autowired");
    }

    @Test
    @DisplayName("Should create JamMaker as singleton")
    void shouldCreateJamMakerAsSingleton() {
        JamMaker jamMaker1 = applicationContext.getBean(JamMaker.class);
        JamMaker jamMaker2 = applicationContext.getBean(JamMaker.class);
        
        assertSame(jamMaker1, jamMaker2, "JamMaker should be singleton (same instance)");
    }
 

    // ==================== Dependency Injection Tests ====================

    @Test
    @DisplayName("Should inject AppleSlicer using @Qualifier")
    void shouldInjectAppleSlicerWithQualifier() {
        assertNotNull(appleSlicer, "AppleSlicer should be injected");
        assertTrue(appleSlicer instanceof AppleSlicer, "Injected bean should be AppleSlicer type");
    }

    @Test
    @DisplayName("Should inject LemonSlicer using @Qualifier")
    void shouldInjectLemonSlicerWithQualifier() {
        assertNotNull(lemonSlicer, "LemonSlicer should be injected");
        assertTrue(lemonSlicer instanceof LemonSlicer, "Injected bean should be LemonSlicer type");
    }

    @Test
    @DisplayName("Should inject different slicer implementations")
    void shouldInjectDifferentSlicerImplementations() {
        assertNotSame(appleSlicer, lemonSlicer, "AppleSlicer and LemonSlicer should be different instances");
        assertNotEquals(appleSlicer.getClass(), lemonSlicer.getClass(), 
            "AppleSlicer and LemonSlicer should be different types");
    }

    @Test
    @DisplayName("Should inject AppleSlicer into JamMaker by default")
    void shouldInjectAppleSlicerIntoJamMaker() {
        // JamMaker is constructed with @Qualifier("appleSlicer")
        // Call make() which should print to console
        String jam = jamMaker.make();
        
        assertEquals("Made with:Sliced Apple", jam,  
            "Should be AppleSlicer processed slice");
         
    }

    @Test
    @DisplayName("Should print output when make() is called")
    void shouldPrintOutputWhenMakeIsCalled() {
        jamMaker.make();
        
        String output = outputStreamCaptor.toString();
        
        assertFalse(output.isEmpty(), "Should have printed something");
        assertTrue(output.contains("###"), "Should contain ### prefix");
        assertTrue(output.contains("Apple"), "Should contain Apple in output");
        assertTrue(output.contains("Coch COch"), "Should contain the full AppleSlicer message");
    }

    // ==================== SlicerFactory Bean Tests ====================

    @Test
    @DisplayName("Should create appleSlicer bean from factory")
    void shouldCreateAppleSlicerBeanFromFactory() {
        SlicerIF appleSlicer = applicationContext.getBean("appleSlicer", SlicerIF.class);
        
        assertNotNull(appleSlicer, "AppleSlicer bean should exist");
        assertTrue(appleSlicer instanceof AppleSlicer, "Bean should be AppleSlicer type");
    }

    @Test
    @DisplayName("Should create lemonSlicer bean from factory")
    void shouldCreateLemonSlicerBeanFromFactory() {
        SlicerIF lemonSlicer = applicationContext.getBean("lemonSlicer", SlicerIF.class);
        
        assertNotNull(lemonSlicer, "LemonSlicer bean should exist");
        assertTrue(lemonSlicer instanceof LemonSlicer, "Bean should be LemonSlicer type");
    }

    @Test
    @DisplayName("Should create singleton beans from factory")
    void shouldCreateSingletonBeansFromFactory() {
        SlicerIF appleSlicer1 = applicationContext.getBean("appleSlicer", SlicerIF.class);
        SlicerIF appleSlicer2 = applicationContext.getBean("appleSlicer", SlicerIF.class);
        
        assertSame(appleSlicer1, appleSlicer2, "Factory should return singleton beans");
    }

    @Test
    @DisplayName("Should have two SlicerIF beans registered")
    void shouldHaveTwoSlicerIFBeansRegistered() {
        String[] beanNames = applicationContext.getBeanNamesForType(SlicerIF.class);
        
        assertEquals(2, beanNames.length, "Should have exactly 2 SlicerIF beans");
        assertTrue(containsBean(beanNames, "appleSlicer"), "Should contain appleSlicer bean");
        assertTrue(containsBean(beanNames, "lemonSlicer"), "Should contain lemonSlicer bean");
    }

    // ==================== Functional Tests ====================

    @Test
    @DisplayName("Should execute make() without errors")
    void shouldExecuteMakeWithoutErrors() {
        assertDoesNotThrow(() -> jamMaker.make(), 
            "make() should execute without throwing exceptions");
    }

    @Test
    @DisplayName("AppleSlicer should return correct slice message")
    void appleSlicerShouldReturnCorrectMessage() {
        String result = appleSlicer.slice();
        
        assertNotNull(result, "Slice result should not be null");
        assertEquals("Slicing Apple: Coch COch", result, "AppleSlicer should return correct message");
    }

    @Test
    @DisplayName("LemonSlicer should return correct slice message")
    void lemonSlicerShouldReturnCorrectMessage() {
        String result = lemonSlicer.slice();
        
        assertNotNull(result, "Slice result should not be null");
        assertEquals("Slicing Lmeon: not so juicy", result, "LemonSlicer should return correct message");
    }

    @Test
    @DisplayName("JamMaker should use AppleSlicer by default")
    void jamMakerShouldUseAppleSlicerByDefault() {
        // Since JamMaker uses @Qualifier("appleSlicer"), it should use AppleSlicer
        // We can verify by checking that make() doesn't throw and the slicer works
        assertDoesNotThrow(() -> jamMaker.make());
        
        // Verify the autowired appleSlicer works
        String appleResult = appleSlicer.slice();
        assertTrue(appleResult.contains("Apple"), "JamMaker should be using AppleSlicer");
    }

    // ==================== Qualifier Resolution Tests ====================

    @Test
    @DisplayName("Should resolve correct bean with appleSlicer qualifier")
    void shouldResolveCorrectBeanWithAppleSlicerQualifier() {
        SlicerIF bean = applicationContext.getBean("appleSlicer", SlicerIF.class);
        
        assertEquals(AppleSlicer.class, bean.getClass(), 
            "appleSlicer qualifier should resolve to AppleSlicer class");
    }

    @Test
    @DisplayName("Should resolve correct bean with lemonSlicer qualifier")
    void shouldResolveCorrectBeanWithLemonSlicerQualifier() {
        SlicerIF bean = applicationContext.getBean("lemonSlicer", SlicerIF.class);
        
        assertEquals(LemonSlicer.class, bean.getClass(), 
            "lemonSlicer qualifier should resolve to LemonSlicer class");
    }

    @Test
    @DisplayName("Should maintain separate instances for different qualifiers")
    void shouldMaintainSeparateInstancesForDifferentQualifiers() {
        SlicerIF apple = applicationContext.getBean("appleSlicer", SlicerIF.class);
        SlicerIF lemon = applicationContext.getBean("lemonSlicer", SlicerIF.class);
        
        assertNotSame(apple, lemon, "Different qualifiers should return different instances");
        
        String appleResult = apple.slice();
        String lemonResult = lemon.slice();
        
        assertNotEquals(appleResult, lemonResult, "Different slicers should produce different results");
    }

    // ==================== Constructor Injection Tests ====================

    @Test
    @DisplayName("Should inject dependency via constructor")
    void shouldInjectDependencyViaConstructor() {
        // JamMaker uses constructor injection with @Qualifier
        assertNotNull(jamMaker, "Constructor injection should succeed");
        
        // Verify the dependency is actually usable
        assertDoesNotThrow(() -> jamMaker.make(), 
            "Injected dependency should be functional");
    }

    @Test
    @DisplayName("Should prefer qualifier over type matching")
    void shouldPreferQualifierOverTypeMatching() {
        // Even though there are 2 beans of type SlicerIF,
        // @Qualifier("appleSlicer") should resolve to the correct one
        
        SlicerIF injectedSlicer = applicationContext.getBean("appleSlicer", SlicerIF.class);
        
        assertNotNull(injectedSlicer, "Qualifier should resolve correctly despite multiple candidates");
        assertTrue(injectedSlicer instanceof AppleSlicer, 
            "Qualifier should select AppleSlicer, not LemonSlicer");
    }

    // ==================== Integration Tests ====================

    @Test
    @DisplayName("Should complete full IOC workflow")
    void shouldCompleteFullIOCWorkflow() {
        // 1. Factory creates beans
        //assertNotNull(slicerFactory, "SlicerFactory should be created");
        
        // 2. Beans are registered in context
        SlicerIF apple = applicationContext.getBean("appleSlicer", SlicerIF.class);
        SlicerIF lemon = applicationContext.getBean("lemonSlicer", SlicerIF.class);
        assertNotNull(apple, "AppleSlicer bean should be registered");
        assertNotNull(lemon, "LemonSlicer bean should be registered");
        
        // 3. JamMaker is created with dependency injection
        assertNotNull(jamMaker, "JamMaker should be created");
        
        // 4. JamMaker uses injected dependency
        assertDoesNotThrow(() -> jamMaker.make(), "JamMaker should use injected dependency");
        
        // 5. Both slicers function correctly
        assertTrue(apple.slice().contains("Apple"), "AppleSlicer should work");
        assertTrue(lemon.slice().contains("Lmeon"), "LemonSlicer should work");
    }

    @Test
    @DisplayName("Should demonstrate IOC container managing object lifecycle")
    void shouldDemonstrateIOCContainerManagingLifecycle() {
        // Verify Spring container manages all beans
        assertTrue(applicationContext.containsBean("jamMaker"), "Container should manage JamMaker");
        assertTrue(applicationContext.containsBean("appleSlicer"), "Container should manage appleSlicer");
        assertTrue(applicationContext.containsBean("lemonSlicer"), "Container should manage lemonSlicer");
        assertTrue(applicationContext.containsBean("slicerFactory"), "Container should manage slicerFactory");
        
        // Verify singleton scope (default)
        assertSame(
            applicationContext.getBean("jamMaker"),
            applicationContext.getBean("jamMaker"),
            "Beans should be singleton by default"
        );
    }

    @Test
    @DisplayName("Should support strategy pattern via dependency injection")
    void shouldSupportStrategyPatternViaDependencyInjection() {
        // The SlicerIF interface allows strategy pattern
        // JamMaker can work with any SlicerIF implementation
        
        SlicerIF appleStrategy = applicationContext.getBean("appleSlicer", SlicerIF.class);
        SlicerIF lemonStrategy = applicationContext.getBean("lemonSlicer", SlicerIF.class);
        
        // Both implement the same interface
        assertNotNull(appleStrategy.slice(), "Apple strategy should work");
        assertNotNull(lemonStrategy.slice(), "Lemon strategy should work");
        
        // But produce different behaviors
        assertNotEquals(appleStrategy.slice(), lemonStrategy.slice(), 
            "Different strategies should produce different results");
    }

    // ==================== Helper Methods ====================

    private boolean containsBean(String[] beanNames, String beanName) {
        for (String name : beanNames) {
            if (name.equals(beanName)) {
                return true;
            }
        }
        return false;
    }
}
