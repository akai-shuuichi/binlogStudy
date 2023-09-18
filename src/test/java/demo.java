import org.junit.Test;

public class demo {

    @Test
    public void t1() {
        test(1023, 2);
    }

    void test(int val, int len) {
        for(int i=0;i<len;i++){
            System.out.println(0x000000FF & (val >> (i << 3)));
        }
    }
}
