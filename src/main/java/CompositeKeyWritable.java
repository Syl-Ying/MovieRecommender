import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
    private Text movieA;
    private DoubleWritable score;

    public CompositeKeyWritable() {
        this.movieA = new Text();
        this.score = new DoubleWritable();
    }

    public CompositeKeyWritable(String movieA, double score) {
        this.movieA = new Text(movieA);
        this.score = new DoubleWritable(score);
    }

    public Text getMovieA() {
        return movieA;
    }

    public DoubleWritable getScore() {
        return score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        movieA.write(out);
        score.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movieA.readFields(in);
        score.readFields(in);
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int cmp = movieA.compareTo(o.movieA);
        if (cmp != 0) return cmp;

        // score descending
        return -score.compareTo(o.score);
    }

    @Override
    public String toString() {
        return movieA.toString() + "\t" + score.toString();
    }
}