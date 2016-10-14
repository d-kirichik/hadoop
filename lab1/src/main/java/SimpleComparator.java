import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by seven-teen on 14.10.16.
 */
public class SimpleComparator extends WritableComparator {
    protected SimpleComparator() {
        super(CSVInput.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        CSVInput f = (CSVInput) first;
        CSVInput s = (CSVInput) second;
        return ((int)f.arr_delay - (int)s.arr_delay);
    }

}
