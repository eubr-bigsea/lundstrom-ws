package bigsea;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.io.BufferedReader;
import java.io.InputStreamReader;

@RestController
public class LundstromController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping("/lundstrom/input/{executors}/{cores}/{memory}/{datasize}/{application}")
    public String lundstrom(@PathVariable int executors, @PathVariable String cores, @PathVariable String memory, @PathVariable String datasize, @PathVariable String application) throws java.io.IOException {
    	Process p = Runtime.getRuntime().exec("python ../spark-lundstrom/run.py "+executors+" "+cores+" "+memory+" "+datasize+" "+application);
    	BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String ret = in.readLine() + "\n";
		return ret;
    }
}
