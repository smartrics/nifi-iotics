package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.iotics.api.Property;
import com.iotics.api.UpsertTwinRequest;
import org.hamcrest.CoreMatchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.UriConstants;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

public class FollowerTwinTest {

    public static final String ONT_PREFIX = "http://hello.com/ont#";
    private final Identity agentIdentity = new Identity("aKey", "aName", "did:iotics:234");
    private final Identity myIdentity = new Identity("tKey", "tName", "did:iotics:432");
    private IoticsApi apiMock;
    private SimpleIdentityManager simMock;

    @BeforeEach
    void setUp() {
        apiMock = Mockito.mock(IoticsApi.class);
        simMock = Mockito.mock(SimpleIdentityManager.class);
        when(simMock.agentIdentity()).thenReturn(agentIdentity);
    }

    @Test
    void injectsAgentIdentityAsAppID() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(request.getHeaders().getClientAppId(), is(equalTo(agentIdentity.did())));
    }

    @Test
    void buildsWithTwinIdCorrectly() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(request.getPayload().getTwinId().getId(), CoreMatchers.is(equalTo("did:iotics:432")));
    }

    @Test
    void buildsWithLabel() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(findTwinProperty(UriConstants.RDFSProperty.Label, request).getStringLiteralValue().getValue(), is(equalTo("myLabel")));
    }

    @Test
    void buildsWithComment() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(findTwinProperty(UriConstants.RDFSProperty.Comment, request).getStringLiteralValue().getValue(), is(equalTo("myComment")));
    }

    @Test
    void buildsWithType() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(findTwinProperty(UriConstants.RDFProperty.Type, request).getUriValue().getValue(), is(equalTo("myType")));
    }

    @Test
    void buildsWithVisibility() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(findTwinProperty(UriConstants.IOTICSProperties.HostAllowListName, request).getUriValue().getValue(),
                is(equalTo(UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString())));
    }

    @Test
    void buildsWithFeed() {
        FollowerTwin twin = aFollowerTwin();
        UpsertTwinRequest request = twin.getUpsertTwinRequest();
        assertThat(request.getPayload().getFeeds(0).getId(), is(equalTo("status")));
        assertThat(findFeedProperty("status", UriConstants.RDFSProperty.Label, request).getStringLiteralValue().getValue(),
                is(equalTo("OperationalStatus")));
        assertThat(findFeedProperty("status", UriConstants.RDFSProperty.Comment, request).getStringLiteralValue().getValue(),
                is(equalTo("Current operational status of this twin")));
    }

    @Test
    void buildsFeedPayload() {
        FollowerTwin twin = aFollowerTwin();
        String data = twin.getShareFeedDataRequest().getFirst().getPayload().getSample().getData().toStringUtf8();
        Gson gson = new Gson();
        FollowerTwin.OperationalStatus os = gson.fromJson(data, FollowerTwin.OperationalStatus.class);
        assertThat(os.isOperational(), is(equalTo(true)));
    }

    private Property findTwinProperty(String key, UpsertTwinRequest request) {
        List<Property> propertiesList = request.getPayload().getPropertiesList();
        return propertiesList.stream().filter(property -> property.getKey().equals(key)).findFirst().orElseThrow();
    }

    private Property findFeedProperty(String feedId, String key, UpsertTwinRequest request) {
        List<Property> propertiesList = request.getPayload().getFeedsList().stream().filter(upsertFeedWithMeta -> upsertFeedWithMeta.getId().equals(feedId)).findFirst().orElseThrow().getPropertiesList();
        return propertiesList.stream().filter(property -> property.getKey().equals(key)).findFirst().orElseThrow();
    }


    private @NotNull FollowerTwin aFollowerTwin() {
        return new FollowerTwin(new FollowerTwin.FollowerModel("myLabel", "myComment", "myType"), this.apiMock, this.simMock, this.myIdentity);
    }


}
