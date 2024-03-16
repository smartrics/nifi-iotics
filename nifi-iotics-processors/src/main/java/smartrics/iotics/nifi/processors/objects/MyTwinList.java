package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.SearchResponse;

import java.util.List;

public record MyTwinList(List<MyTwin> twins) {

    public MyTwinList(SearchResponse searchResponse) {
        this(searchResponse.getPayload().getTwinsList().stream().map(MyTwin::new).toList());

    }
}
