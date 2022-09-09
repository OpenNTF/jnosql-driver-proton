/**
 * Copyright © 2022 Jesse Gallagher
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openntf.xsp.nosql.communication.driver.proton.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.eclipse.jnosql.mapping.reflection.ClassMapping;
import org.openntf.xsp.nosql.communication.driver.DominoConstants;
import org.openntf.xsp.nosql.communication.driver.impl.AbstractDominoDocumentCollectionManager;
import org.openntf.xsp.nosql.communication.driver.impl.DQL;
import org.openntf.xsp.nosql.communication.driver.impl.QueryConverter;
import org.openntf.xsp.nosql.communication.driver.impl.DQL.DQLTerm;
import org.openntf.xsp.nosql.communication.driver.impl.QueryConverter.QueryConverterResult;
import org.openntf.xsp.nosql.communication.driver.proton.AccessTokenSupplier;
import org.openntf.xsp.nosql.communication.driver.proton.DatabaseSupplier;
import org.openntf.xsp.nosql.mapping.extension.ViewQuery;

import com.hcl.domino.db.model.BulkOperationException;
import com.hcl.domino.db.model.ComputeOptions;
import com.hcl.domino.db.model.Database;
import com.hcl.domino.db.model.Document;
import com.hcl.domino.db.model.OptionalAccessToken;
import com.hcl.domino.db.model.OptionalArg;
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
	private final AccessTokenSupplier tokenSupplier;
	private final ProtonEntityConverter entityConverter;
	
	public ProtonDocumentCollectionManager(DatabaseSupplier supplier, AccessTokenSupplier tokenSupplier) {
		this.supplier = supplier;
		this.tokenSupplier = tokenSupplier;
		this.entityConverter = new ProtonEntityConverter();
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
		ClassMapping mapping = getClassMapping(entity.getName());
		Database database = supplier.get();
		try {
			Document doc = entityConverter.convertNoSQLEntity(entity, true, mapping);
			doc = database.createDocument(doc, composeArgs(new ComputeOptions(computeWithForm, true))).get();
			entity.add(jakarta.nosql.document.Document.of(DominoConstants.FIELD_ID, doc.getUnid()));
			return entity;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public DocumentEntity update(DocumentEntity entity, boolean computeWithForm) {
		Optional<jakarta.nosql.document.Document> maybeId = entity.find(DominoConstants.FIELD_ID);
		if(!maybeId.isPresent()) {
			// Then consider it an insert
			return insert(entity, computeWithForm);
		} else {
			ClassMapping mapping = getClassMapping(entity.getName());
			Database database = supplier.get();
			try {
				Document doc = entityConverter.convertNoSQLEntity(entity, true, mapping);
				DQLTerm dql = DQL.item("@Text(@DocumentUniqueID)").isEqualTo(maybeId.get().get(String.class)); //$NON-NLS-1$
				doc = database.upsertDocument(dql.toString(), doc, composeArgs(new ComputeOptions(computeWithForm, true))).get();
				return entity;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public boolean existsById(String unid) {
		Database database = supplier.get();
		try {
			Document doc = database.readDocumentByUnid(unid, Collections.emptyList(), composeArgs()).get();
			return doc != null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Optional<DocumentEntity> getByNoteId(String entityName, String noteId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<DocumentEntity> getById(String entityName, String id) {
		ClassMapping mapping = getClassMapping(entityName);
		Database database = supplier.get();
		try {
			List<String> itemNames = getItemNames(mapping);
			
			Document doc = database.readDocumentByUnid(id, itemNames, composeArgs()).get();
			
			return entityConverter.convertDocuments(entityName, Arrays.asList(doc), mapping)
				.findFirst();
		} catch (BulkOperationException e) {
			// Assume it doesn't exist
			return Optional.empty();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
		return StreamSupport.stream(entities.spliterator(), false)
			.map(entity -> insert(entity, false))
			.collect(Collectors.toList());
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
				database.deleteDocumentsByUnid((Set<String>)unids, composeArgs()).get();
			} else if(query.getCondition().isPresent()) {
				// Then do it via DQL
				DQLTerm dql = QueryConverter.getCondition(query.getCondition().get());
				database.deleteDocuments(dql.toString(), composeArgs()).get();
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
			List<String> itemNames = getItemNames(mapping);
			OptionalItemNames itemNamesArg = new OptionalItemNames(itemNames);
			OptionalStart startArg = new OptionalStart((int)skip);
			OptionalCount countArg = new OptionalCount(limit < 1 ? Integer.MAX_VALUE : (int)limit);
			
			List<Document> docs = database.readDocuments(
				queryResult.getStatement().toString(),
				composeArgs(
					itemNamesArg,
					startArg,
					countArg
				)
			).get();
			
			return entityConverter.convertDocuments(entityName, docs, mapping);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long count(String documentCollection) {
		DQLTerm dql = DQL.item(DominoConstants.FIELD_NAME).isEqualTo(documentCollection);
		Database database = supplier.get();
		try {
			return database.readDocuments(dql.toString(), composeArgs()).get().size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		
	}

	private List<String> getItemNames(ClassMapping mapping) {
		return mapping.getFields()
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
	}
	
	private OptionalArg[] composeArgs(OptionalArg... args) {
		List<OptionalArg> result = new ArrayList<>();
		
		String token = tokenSupplier.get();
		if(token != null && !token.isEmpty()) {
			result.add(new OptionalAccessToken(token));
		}
		
		if(args != null && args.length > 0) {
			result.addAll(Arrays.asList(args));
		}
		
		return result.toArray(new OptionalArg[result.size()]);
	}
}
