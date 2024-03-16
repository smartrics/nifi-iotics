package smartrics.iotics.nifi.processors.objects;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.iotics.api.*;
import org.apache.nifi.logging.ComponentLog;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.space.Builders;
import smartrics.iotics.space.grpc.IoticsApi;
import smartrics.iotics.space.twins.AbstractTwin;
import smartrics.iotics.space.twins.MappableMaker;
import smartrics.iotics.space.twins.Mapper;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Executor;

public class ConcreteTwin extends AbstractTwin implements MappableMaker<JsonObject>, Mapper<JsonObject> {

    private final JsonObject source;
    private final String ontPrefix;
    private final ComponentLog logger;

    public ConcreteTwin(ComponentLog logger, IoticsApi api, JsonObject source, String ontPrefix, String keyName, Executor executor) {
        super(api, keyName, executor);
        this.source = source;
        this.ontPrefix = ontPrefix;
        this.logger = logger;
    }

    @Override
    public Mapper<JsonObject> getMapper() {
        return this;
    }

    @Override
    public JsonObject getTwinSource() {
        return source;
    }

    @Override
    public Identity getTwinIdentity(JsonObject jsonObject) {
        return this.getIdentity();
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest(JsonObject jsonObject) {
        UpsertTwinRequest.Builder reqBuilder = UpsertTwinRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(super.ioticsApi().getSim().agentIdentity().did()));
        Identity id = this.getIdentity();
        UpsertTwinRequest.Payload.Builder payloadBuilder = UpsertTwinRequest.Payload.newBuilder();
        payloadBuilder.setTwinId(TwinID.newBuilder().setId(id.did()));
        payloadBuilder.addProperties(Property.newBuilder().setKey("http://data.iotics.com/public#hostAllowList").setUriValue(Uri.newBuilder().setValue("http://data.iotics.com/public#allHosts").build()).build());

        jsonObject.keySet().forEach(s -> {
            JsonElement el = jsonObject.get(s);
            if(el.isJsonPrimitive()) {
                Property.Builder p;
                try {
                    p = Property.newBuilder().setKey(asUri(ontPrefix, s).toString());
                    if(el.getAsJsonPrimitive().isBoolean()) {
                        p.setLiteralValue(Literal.newBuilder().setDataType("boolean").setValue(el.getAsString().trim()).build());
                    } else if(el.getAsJsonPrimitive().isNumber()) {
                        p.setLiteralValue(Literal.newBuilder().setDataType("decimal").setValue(el.getAsString().trim()).build());
                    } else if(el.getAsJsonPrimitive().isString()) {
                        try {
                            URI uri = URI.create(el.getAsString().trim());
                            if(uri.getScheme() == null) {
                                p.setStringLiteralValue(StringLiteral.newBuilder().setValue(el.getAsString().trim()));
                            } else {
                                p.setUriValue(Uri.newBuilder().setValue(uri.toString()));
                            }
                        } catch (Exception e) {
                            p.setStringLiteralValue(StringLiteral.newBuilder().setValue(el.getAsString().trim()));
                        }
                        // should parse date and timestamps
                    }
                    payloadBuilder.addProperties(p);
                } catch (InvalidProtocolBufferException e) {
                    logger.info("invalid key: " + s , e);
                }
            }
        });


        UpsertTwinRequest.Payload payload = payloadBuilder.build();
        reqBuilder.setPayload(payload);
        return reqBuilder.build();
    }

    private URI asUri(String prefix, String s) throws InvalidProtocolBufferException {
        try {
            return URI.create(s);
        } catch (Exception e) {
            return URI.create(String.join(prefix, "#", s));
        }
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest(JsonObject jsonObject) {
        return null;
    }
}
