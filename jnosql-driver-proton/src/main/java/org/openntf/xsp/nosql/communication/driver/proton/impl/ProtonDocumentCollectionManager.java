package org.openntf.xsp.nosql.communication.driver.proton.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.jnosql.mapping.reflection.ClassMapping;
import org.openntf.xsp.nosql.communication.driver.DominoConstants;
import org.openntf.xsp.nosql.communication.driver.impl.AbstractDominoDocumentCollectionManager;
import org.openntf.xsp.nosql.communication.driver.impl.QueryConverter;
import org.openntf.xsp.nosql.communication.driver.impl.DQL.DQLTerm;
import org.openntf.xsp.nosql.communication.driver.impl.QueryConverter.QueryConverterResult;
import org.openntf.xsp.nosql.communication.driver.proton.DatabaseSupplier;
import org.openntf.xsp.nosql.mapping.extension.ViewQuery;

import com.hcl.domino.db.model.BulkOperationException;
import com.hcl.domino.db.model.Database;
import com.hcl.domino.db.model.Document;
import com.hcl.domino.db.model.OptionalCount;
import com.hcl.domino.db.model.OptionalItemNames;
import com.hcl.domino.db.model.OptionalStart;

import jakarta.nosql.Sort;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import jakarta.nosql.mapping.Column;
import jakarta.nosql.mapping.Pagination;
import jakarta.nosql.mapping.Sorts;

public class ProtonDocumentCollectionManager extends AbstractDominoDocumentCollectionManager {
	
	private final DatabaseSupplier supplier;
	
	public ProtonDocumentCollectionManager(DatabaseSupplier supplier) {
		this.supplier = supplier;
	}

	@Override
	public Stream<DocumentEntity> viewEntryQuery(String entityName, String viewName, Pagination pagination, Sorts sorts,
			int maxLevel, boolean docsOnly, ViewQuery viewQuery, boolean singleResult) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<DocumentEntity> viewDocumentQuery(String entityName, String viewName, Pagination pagination,
			Sorts sorts, int maxLevel, ViewQuery viewQuery, boolean singleResult) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putInFolder(String entityId, String folderName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeFromFolder(String entityId, String folderName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DocumentEntity insert(DocumentEntity entity, boolean computeWithForm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentEntity update(DocumentEntity entity, boolean computeWithForm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean existsById(String unid) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Optional<DocumentEntity> getByNoteId(String entityName, String noteId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<DocumentEntity> getById(String entityName, String id) {
		// TODO Auto-generated method stub
		return Optional.empty();
	}

	@Override
	public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(DocumentDeleteQuery query) {
		try {
			Database database = supplier.get();
			Collection<String> unids = query.getDocuments();
			if(unids != null) {
				unids = unids.stream()
					.filter(unid -> unid != null && !unid.isEmpty())
					.collect(Collectors.toSet());
			}
			if(unids != null && !unids.isEmpty()) {
				database.deleteDocumentsByUnid((Set<String>)unids).get();
			} else if(query.getCondition().isPresent()) {
				// Then do it via DQL
				DQLTerm dql = QueryConverter.getCondition(query.getCondition().get());
				database.deleteDocuments(dql.toString()).get();
			}
		} catch (BulkOperationException | InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Stream<DocumentEntity> select(DocumentQuery query) {
		String entityName = query.getDocumentCollection();
		ClassMapping mapping = getClassMapping(entityName);
		
		QueryConverterResult queryResult = QueryConverter.select(query);
		
		long skip = queryResult.getSkip();
		long limit = queryResult.getLimit();
		
		// Sorting is not available in queries, and this is left as a reminder
		//   if intentionally ignoring them
		@SuppressWarnings("unused")
		List<Sort> sorts = query.getSorts();
		
		Database database = supplier.get();
		try {
			List<String> itemNames = mapping.getFields()
				.stream()
				.map(f -> f.getNativeField())
				.map(f -> {
					Column col = f.getAnnotation(Column.class);
					if(col == null) {
						return null;
					}
					return col.value().isEmpty() ? f.getName() : col.value();
				})
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
			OptionalItemNames itemNamesArg = new OptionalItemNames(itemNames);
			OptionalStart startArg = new OptionalStart((int)skip);
			OptionalCount countArg = new OptionalCount(limit < 1 ? Integer.MAX_VALUE : (int)limit);
			
			List<Document> docs = database.readDocuments(
				queryResult.getStatement().toString(),
				itemNamesArg,
				startArg,
				countArg
			).get();
			
			return docs.stream()
				.map(doc -> {
					String id = doc.getUnid();
					
					List<jakarta.nosql.document.Document> resultDocs = new ArrayList<>();
					resultDocs.add(jakarta.nosql.document.Document.of(DominoConstants.FIELD_ID, id));
					
					doc.getItems().forEach(item -> {
						List<?> val = item.getValue();
						Object value = val == null || val.isEmpty() ? null : val.size() == 1 ? val.get(0) : val;
						resultDocs.add(jakarta.nosql.document.Document.of(item.getName(), value));
					});
					
					return DocumentEntity.of(entityName, resultDocs);
				});
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public long count(String documentCollection) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() {
		
	}

}
