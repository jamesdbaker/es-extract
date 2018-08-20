package jamesbaker.extract;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

public class ExtractProcessorTest extends ESTestCase {

  public void testEmails() throws Exception {
    Map<String, Object> document = new HashMap<>();
    document.put("source_field", "John (john@example.com) e-mailed jane@example.com last week.");
    document.put("another_source_field", "mary@example.com");
    document.put("dont_process", "peter@example.com");

    IngestDocument ingestDocument = new IngestDocument(document, Collections.emptyMap());

    ExtractProcessor processor = new ExtractProcessor("abcdefghij", Set
        .of("source_field", "another_source_field"), "target_field");
    processor.execute(ingestDocument);
    Map<String, Object> data = ingestDocument.getSourceAndMetadata();

    List<String> extracted = (List<String>) data.get("target_field");
    assertThat(extracted, containsInAnyOrder("john@example.com", "jane@example.com", "mary@example.com"));
    assertThat(extracted, not(contains("peter@example.com")));
  }
}
