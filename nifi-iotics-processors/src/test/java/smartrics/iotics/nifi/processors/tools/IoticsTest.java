package smartrics.iotics.nifi.processors.tools;

import com.google.common.util.concurrent.ListenableFuture;
import com.iotics.api.ListAllTwinsRequest;
import com.iotics.api.ListAllTwinsResponse;
import org.junit.jupiter.api.Test;
import smartrics.iotics.host.Builders;
import smartrics.iotics.nifi.services.Configuration;
import smartrics.iotics.nifi.services.Iotics;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import static smartrics.iotics.nifi.services.BasicIoticsHostService.TOKEN_DURATION;

class IoticsTest {

    private static Configuration newConfiguration(int tokenDuration) throws IOException {
        Map<String, String> conf = HashMap.newHashMap(5);
        Properties prop = new Properties();
        // The path to your properties file
        String propFileName = ".env";
        Path absolutePath = Paths.get(propFileName).toAbsolutePath();
        try (InputStream inputStream = new FileInputStream(absolutePath.toFile())) {
            prop.load(inputStream);
            for (String key : prop.stringPropertyNames()) {
                conf.put(key, prop.getProperty(key));
            }
        }
        conf.put(TOKEN_DURATION.getName(), Integer.toString(tokenDuration));
        return new Configuration(conf);
    }

    @Test
    void validToken() throws Exception {
        Iotics iotics = Iotics.Builder.newBuilder().withConfiguration(newConfiguration(3)).build();
        for(int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            System.out.println("Iteration " + i);
            ListenableFuture<ListAllTwinsResponse> res = iotics.api().twinAPIFuture().listAllTwins(ListAllTwinsRequest.newBuilder()
                    .setHeaders(Builders.newHeadersBuilder(iotics.sim().agentIdentity()))
                    .build());
            res.addListener(() -> {
                try {
                    System.out.println(res.resultNow());
                } catch (Exception e) {
                    System.err.println(res.exceptionNow().toString());
                }
            }, Executors.newFixedThreadPool(10));
        }

    }

}