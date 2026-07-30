package com.learn.ioc_map;

import org.springframework.stereotype.Component;

@Component("apple")
public class AppleSlicer implements SlicerIF 
{
    @Override
    public String slice() {
         return "Sliced Apple";
    }
    
}
