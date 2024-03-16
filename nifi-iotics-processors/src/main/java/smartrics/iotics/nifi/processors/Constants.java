package smartrics.iotics.nifi.processors;

import com.iotics.api.Scope;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.util.Arrays;

public interface Constants {
    String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    String RDFS = "http://www.w3.org/2000/01/rdf-schema#";

    enum SCOPE {
        LOCAL, GLOBAL
    }

    PropertyDescriptor QUERY_SCOPE = new PropertyDescriptor.Builder()
            .name("queryScope")
            .displayName("Query Scope")
            .description("query scope, LOCAL for executing only on the local host or GLOBAL for the whole network")
            .allowableValues(Arrays.stream(SCOPE.values())
                    .map(enumValue -> new AllowableValue(enumValue.name(), enumValue.name()))
                    .toArray(AllowableValue[]::new))
            .required(true)
            .defaultValue(Scope.GLOBAL.name())
            .build();

    PropertyDescriptor IOTICS_HOST_SERVICE = new PropertyDescriptor.Builder()
            .name("IOTICS Host Service")
            .description("Service configuring and providing access an IOTICS host")
            .identifiesControllerService(IoticsHostService.class)
            .required(true)
            .build();

    Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();


}
