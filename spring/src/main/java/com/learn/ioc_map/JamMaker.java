package com.learn.ioc_map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
 

@Component
public class JamMaker {
    
    private final SlicerIF slicer;

    public JamMaker(@Qualifier("apple") SlicerIF slicer){
        this.slicer = slicer;
    }

    public String make(){
       String slice =  slicer.slice();
       return "Made with:"+slice;
    }
}
