package com.learn.ioc_map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

 
public class SlicerFactory {
    
    @Bean @Qualifier("aSlicer")
    public SlicerIF appleSlicer(){
        return new AppleSlicer();
    }

    @Bean @Qualifier("lSlicer")
    public SlicerIF lemonSlicer(){
        return new LemonSlicer();
    }
}
