package se.motility.ziploq.testapi;

import java.util.Comparator;
import java.util.List;

import se.motility.ziploq.api.Ziploq;

public interface ProducerState {
    
    Ziploq<Object> ziploq();
    
    List<Producer> producers();
    
    Comparator<Object> comparator();
}
