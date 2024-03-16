package smartrics.iotics.nifi.services;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import smartrics.iotics.space.grpc.IoticsApi;

import java.util.concurrent.ExecutorService;

@Tags({"IOTICS"})
@CapabilityDescription("Basic and low security connection to an IOTICS host")
public interface IoticsHostService extends ControllerService {
    ExecutorService getExecutor();

    IoticsApi getIoticsApi();
}
