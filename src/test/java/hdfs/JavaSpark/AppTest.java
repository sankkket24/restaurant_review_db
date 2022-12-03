package hdfs.JavaSpark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
public class AppTest {

    @Test
    void testHello(CapturedOutput capture) throws Exception {
        assertEquals(1,1);
    }
}
