package com.learn.ioc_map;

import org.springframework.stereotype.Component;

@Component("lemon")
public class LemonSlicer implements SlicerIF {

    @Override
    public String slice() {
        // TODO Auto-generated method stub
        return "Sliced Lemon";
    }
    
}
