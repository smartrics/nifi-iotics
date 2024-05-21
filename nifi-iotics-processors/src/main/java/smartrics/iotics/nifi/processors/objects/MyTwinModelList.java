package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.SearchResponse;

import java.util.List;

public record MyTwinModelList(List<MyTwinModel> twins) {

    public MyTwinModelList(SearchResponse searchResponse) {
        this(searchResponse.getPayload().getTwinsList().stream().map(MyTwinModel::new).toList());

    }
}
