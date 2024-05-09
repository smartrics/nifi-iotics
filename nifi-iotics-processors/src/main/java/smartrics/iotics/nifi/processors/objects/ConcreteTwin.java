package smartrics.iotics.nifi.processors.objects;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.iotics.api.*;
import org.apache.nifi.logging.ComponentLog;
import smartrics.iotics.connectors.twins.AbstractTwin;
import smartrics.iotics.connectors.twins.MappableMaker;
import smartrics.iotics.connectors.twins.Mapper;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.UriConstants;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.tools.DateTypeDetector;

import java.net.URI;
import java.util.List;

public class ConcreteTwin extends AbstractTwin implements MappableMaker, Mapper {

    private final JsonObject source;
    private final String ontPrefix;
    private final ComponentLog logger;

    public ConcreteTwin(ComponentLog logger, IoticsApi api, SimpleIdentityManager sim, JsonObject source, String ontPrefix, Identity myIdentity) {
        super(api, sim, myIdentity);
        this.source = source;
        this.ontPrefix = ontPrefix;
        this.logger = logger;
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
        // should externalise
        payloadBuilder.addProperties(
                Property.newBuilder().setKey(UriConstants.IOTICSProperties.HostAllowListName)
                        .setUriValue(Uri.newBuilder().setValue(UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString())
                                .build())
                        .build());

        this.source.keySet().forEach(s -> {
            JsonElement el = this.source.get(s);
            if (el.isJsonPrimitive()) {
                Property.Builder p;
                try {
                    URI keyUri = asUri(ontPrefix, s);
                    p = Property.newBuilder().setKey(keyUri.toString());
                    if (el.getAsJsonPrimitive().isBoolean()) {
                        p.setLiteralValue(Literal.newBuilder().setDataType("boolean").setValue(el.getAsString().trim()).build());
                    } else if (el.getAsJsonPrimitive().isNumber()) {
                        p.setLiteralValue(Literal.newBuilder().setDataType("decimal").setValue(el.getAsString().trim()).build());
                    } else /* if (el.getAsJsonPrimitive().isString()) */ {
                        String trimmed = el.getAsString().trim();
                        DateTypeDetector.detectDateTimeType(trimmed).ifPresentOrElse(xsdDatatype -> p.setLiteralValue(Literal.newBuilder().setDataType(xsdDatatype.toString()).setValue(trimmed).build()), () -> {
                            try {
                                URI uri = URI.create(trimmed);
                                if (uri.isAbsolute()) {
                                    p.setUriValue(Uri.newBuilder().setValue(uri.toString()));
                                } else {
                                    p.setStringLiteralValue(StringLiteral.newBuilder().setValue(trimmed));
                                }
                            } catch (Exception e) {
                                p.setStringLiteralValue(StringLiteral.newBuilder().setValue(trimmed));
                            }
                        });
                    }
                    payloadBuilder.addProperties(p);
                } catch (InvalidProtocolBufferException e) {
                    logger.info("invalid key: " + s, e);
                }
            }
        });


        UpsertTwinRequest.Payload payload = payloadBuilder.build();
        reqBuilder.setPayload(payload);
        return reqBuilder.build();
    }

    private URI asUri(String prefix, String s) throws InvalidProtocolBufferException {
        try {
            URI temp = URI.create(s);
            if (temp.isAbsolute()) {
                return temp;
            }
        } catch (Exception e) {
            // fall back to prefix
        }
        return URI.create(prefix + s);
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest() {
        return null;
    }

}
