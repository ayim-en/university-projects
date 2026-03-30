public class myCustomComparator extends WritableComparator {
    protected myCustomComparator() {
        super(WordYearKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        WordYearKey k1 = (WordYearKey) w1;
        WordYearKey k2 = (WordYearKey) w2;

        return k1.getWord().compareTo(k2.getWord());

    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        
    }
}