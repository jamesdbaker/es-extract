package jamesbaker.extract;

import static org.elasticsearch.ingest.ConfigurationUtils.readList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

import io.annot8.common.registries.ContentBuilderFactoryRegistry;
import io.annot8.components.processors.regex.Email;
import io.annot8.core.components.responses.ProcessorResponse;
import io.annot8.core.components.responses.ProcessorResponse.Status;
import io.annot8.core.data.Item;
import io.annot8.core.data.ItemFactory;
import io.annot8.defaultimpl.content.SimpleText;
import io.annot8.defaultimpl.factories.SimpleContentBuilderFactoryRegistry;
import io.annot8.defaultimpl.factories.SimpleItemFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

public class ExtractProcessor extends AbstractProcessor {
  public static final String TYPE = "extract";

  private final Set<String> fields;
  private final String targetField;

  private final ContentBuilderFactoryRegistry contentBuilderFactoryRegistry;
  private final ItemFactory itemFactory;

  public ExtractProcessor(String tag, String field, String targetField) {
    this(tag, Set.of(field), targetField);
  }

  public ExtractProcessor(String tag, List<String> fields, String targetField) {
    this(tag, new HashSet<>(fields), targetField);
  }

  public ExtractProcessor(String tag, Set<String> fields, String targetField) {
    super(tag);
    this.fields = fields;
    this.targetField = targetField;

    this.contentBuilderFactoryRegistry = new SimpleContentBuilderFactoryRegistry();
    ((SimpleContentBuilderFactoryRegistry) contentBuilderFactoryRegistry)
        .register(SimpleText.class, new SimpleText.BuilderFactory());

    itemFactory = new SimpleItemFactory(contentBuilderFactoryRegistry);
  }

  @Override
  public void execute(IngestDocument ingestDocument) throws Exception {
    Item item = itemFactory.create();

    List<SimpleText> textContent = new ArrayList<>();
    for(String field : fields){
      String content = ingestDocument.getFieldValue(field, String.class);
      SimpleText text = item.create(SimpleText.class)
          .withName(field)
          .withData(content)
          .save();

      textContent.add(text);
    }

    try(Email email = new Email()) {
      ProcessorResponse emailResponse = email.process(item);

      if(emailResponse.getStatus() != Status.OK){
        throw new Exception("Error extracting e-mail addresses");
      }
    }

    List<String> extracted = new ArrayList<>();
    for(SimpleText text : textContent) {
      text.getAnnotations().getAll().forEach(a ->
        a.getBounds().getData(text).ifPresent(extracted::add)
      );
    }

    ingestDocument.setFieldValue(targetField, extracted);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public static final class Factory implements Processor.Factory {

    @Override
    public ExtractProcessor create(Map<String, Processor.Factory> factories, String tag, Map<String, Object> config) {
      List<String> fields = readList(TYPE, tag, config, "field");
      String targetField = readStringProperty(TYPE, tag, config, "target_field", "extracted");

      return new ExtractProcessor(tag, fields, targetField);
    }
  }
}
