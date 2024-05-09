package smartrics.iotics.nifi.processors.objects;

import smartrics.iotics.connectors.twins.AbstractTwin;
import smartrics.iotics.connectors.twins.AnnotationMapper;
import smartrics.iotics.connectors.twins.MappableMaker;
import smartrics.iotics.connectors.twins.Mapper;
import smartrics.iotics.connectors.twins.annotations.Feed;
import smartrics.iotics.connectors.twins.annotations.PayloadValue;
import smartrics.iotics.connectors.twins.annotations.StringLiteralProperty;
import smartrics.iotics.connectors.twins.annotations.UriProperty;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.UriConstants;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

public class FollowerTwin extends AbstractTwin implements MappableMaker, Mapper, AnnotationMapper {
    private final FollowerModel followerModel;

    public FollowerTwin(FollowerModel model, IoticsApi api, SimpleIdentityManager sim, Identity myIdentity) {
        super(api, sim, myIdentity);
        this.followerModel = model;
    }

    @UriProperty(iri = UriConstants.IOTICSProperties.HostAllowListName)
    public String visibility() {
        return UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString();
    }

    @UriProperty(iri = UriConstants.RDFProperty.Type)
    public String type() {
        return followerModel.type();
    }

    @StringLiteralProperty(iri = UriConstants.RDFSProperty.Label)
    public String label() {
        return followerModel.label();
    }

    @StringLiteralProperty(iri = UriConstants.RDFSProperty.Comment)
    public String comment() {
        return followerModel.comment();
    }

    @Override
    public Mapper getMapper() {
        return this;
    }

    @Feed(id = "status")
    public OperationalStatus newOperationalStatus() {
        return new OperationalStatus(true);
    }

    public record FollowerModel(String label, String comment, String type) {
    }

    public record OperationalStatus(
            @PayloadValue(dataType = "boolean", comment = "true if operational") boolean isOperational) {
        @StringLiteralProperty(iri = UriConstants.RDFSProperty.Label)
        public static final String label = "OperationalStatus";
        @StringLiteralProperty(iri = UriConstants.RDFSProperty.Comment)
        public static final String comment = "Current operational status of this twin";
    }
}
