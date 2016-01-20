package org.talend.dataquality.statistics.numeric.histogram;

public class Range implements Comparable<Range>{

    double lower, upper;

    public Range(double lower, double upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public double getLower() {
        return lower;
    }

    public void setLower(double lower) {
        this.lower = lower;
    }

    public double getUpper() {
        return upper;
    }

    public void setUpper(double upper) {
        this.upper = upper;
    }

    @Override
    public int compareTo(Range another) {
        if(lower<another.lower){
            return -1;
        }else if(lower > another.lower){
            return 1;
        }
        return 0;
    }

}