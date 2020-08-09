package top_occupation.peryear;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class occupationPerYear implements Writable{

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    String occupation;
    String year;

    occupationPerYear(){}

    public occupationPerYear(String occupation, String year){
        this.occupation = occupation;
        this.year = year;
    }

    @Override
    public String toString() {
        return year + " : " + occupation;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(occupation);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.occupation = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }
}
