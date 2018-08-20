package jamesbaker.extract;

import java.util.Map;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.Processor.Factory;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

public class ExtractPlugin extends Plugin implements IngestPlugin {

  @Override
  public Map<String, Factory> getProcessors(Processor.Parameters parameters) {
    return MapBuilder.<String, Processor.Factory>newMapBuilder()
        .put(ExtractProcessor.TYPE, new ExtractProcessor.Factory())
        .immutableMap();
  }
}
