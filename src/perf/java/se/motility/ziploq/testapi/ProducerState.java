package se.motility.ziploq.testapi;

import java.util.Comparator;
import java.util.List;

import se.motility.ziploq.api.ZipFlow;

public interface ProducerState {
    
    ZipFlow<Object> ziploq();
    
    List<Producer> producers();
    
    Comparator<Object> comparator();
}
