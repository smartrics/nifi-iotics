package smartrics.iotics.nifi.processors;

import com.iotics.api.Scope;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.util.Arrays;

public interface Constants {
    String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
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
    PropertyDescriptor ID_PROP = new PropertyDescriptor
            .Builder().name("idProp")
            .displayName("ID property")
            .description("This property should be present in the input flow file to identify the value used to determine the twin Identity. This value is then passed to the Identity library as KeyName.")
            .required(true)
            .defaultValue("http://schema.org/identifier")
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();
    Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();
    Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original flow-file content")
            .build();
    Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();

    enum SCOPE {
        LOCAL, GLOBAL
    }


}
