package smartrics.iotics.nifi.processors.objects;

import com.github.jsonldjava.core.RDFDataset;
import com.iotics.api.*;
import smartrics.iotics.connectors.twins.AbstractTwin;
import smartrics.iotics.connectors.twins.MappableMaker;
import smartrics.iotics.connectors.twins.Mapper;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.UriConstants;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.tools.AllowListEntryValidator;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class JsonLdTwin extends AbstractTwin implements MappableMaker, Mapper {

    private final List<RDFDataset.Quad> source;
    private final String defAllowListValue;

    public JsonLdTwin(IoticsApi api, SimpleIdentityManager sim,
                      List<RDFDataset.Quad> quads, Identity myIdentity,
                      String defAllowListValue) {
        super(api, sim, myIdentity);
        this.source = quads;
        this.defAllowListValue = AllowListEntryValidator.toValidString(defAllowListValue)
                .orElseThrow(() -> new IllegalArgumentException("invalid default for allow list value: " + defAllowListValue));
    }

    @Override
    public Mapper getMapper() {
        return this;
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest() {
        UpsertTwinRequest.Builder reqBuilder = UpsertTwinRequest
                .newBuilder()
                .setHeaders(Builders.newHeadersBuilder(super.getAgentIdentity()));
        UpsertTwinRequest.Payload.Builder payloadBuilder = UpsertTwinRequest.Payload.newBuilder();
        payloadBuilder.setTwinId(TwinID.newBuilder().setId(super.getMyIdentity().did()));

        AtomicReference<String> discoveredAllowListValue = new AtomicReference<>(null);

        source.forEach(quad -> {
            String predicateValue = quad.getPredicate().getValue();
            Property.Builder pBuilder = Property.newBuilder().setKey(predicateValue);
            String objValue = quad.getObject().getValue();
            if (predicateValue.equals(UriConstants.IOTICSProperties.HostAllowListName)) {
                Optional<String> res = AllowListEntryValidator.toValidString(quad.getObject().getValue());
                discoveredAllowListValue.set(res.orElse(defAllowListValue));
            } else {
                if (quad.getObject().isIRI()) {
                    pBuilder.setUriValue(Uri.newBuilder().setValue(quad.getObject().getValue()).build());
                } else if (quad.getObject().isLiteral()) {
                    // is literal
                    if (quad.getObject().getLanguage() != null) {
                        pBuilder.setLangLiteralValue(LangLiteral.newBuilder()
                                .setValue(objValue)
                                .setLang(quad.getObject().getLanguage())
                                .build());
                    } else {
                        Literal.Builder lBuilder = Literal.newBuilder()
                                .setDataType("string") // default
                                .setValue(objValue);
                        if (quad.getObject().getDatatype() != null) {
                            URI dataTypeUri = URI.create(quad.getObject().getDatatype());
                            if (dataTypeUri.isAbsolute()) {
                                lBuilder.setDataType(dataTypeUri.getRawFragment());
                            } else {
                                lBuilder.setDataType(quad.getObject().getDatatype());
                            }
                        }
                        pBuilder.setLiteralValue(lBuilder.build());
                    }
                }
            }
            payloadBuilder.addProperties(pBuilder.build());
        });

        String v = discoveredAllowListValue.get();
        if (v == null) {
            v = defAllowListValue;
        }
        payloadBuilder.addProperties(Property.newBuilder()
                .setKey(UriConstants.IOTICSProperties.HostAllowListName)
                .setUriValue(Uri.newBuilder().setValue(v).build())
                .build());
        UpsertTwinRequest.Payload payload = payloadBuilder.build();
        reqBuilder.setPayload(payload);
        return reqBuilder.build();
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest() {
        return null;
    }

}
