package smartrics.iotics.nifi.processors.helpers;

import com.iotics.api.ShareFeedDataRequest;
import com.iotics.api.UpsertTwinRequest;
import smartrics.iotics.connectors.twins.AbstractTwin;
import smartrics.iotics.connectors.twins.MappableMaker;
import smartrics.iotics.connectors.twins.Mapper;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.util.List;

public class CarTwin extends AbstractTwin implements MappableMaker, Mapper {
    private final CarModel car;

    public CarTwin(CarModel car, IoticsApi api, SimpleIdentityManager sim, Identity identity) {
        super(api, sim, identity);
        this.car = car;
    }

    @Override
    public Mapper getMapper() {
        return this;
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest() {
        UpsertTwinRequest request = UpsertTwinRequest.newBuilder()

                .build();
        return null;
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest() {
        return null;
    }
}

