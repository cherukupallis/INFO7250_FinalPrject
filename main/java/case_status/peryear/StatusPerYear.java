package case_status.peryear;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatusPerYear implements Writable {

    String status;
    String year;

    public StatusPerYear(){}

    public StatusPerYear(String status,String year){
        this.status = status;
        this.year = year;
    }

    @Override
    public String toString() {
        return year+ " : "+ status ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(status);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.status = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }
}
