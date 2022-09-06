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

import static java.util.Objects.requireNonNull;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.jnosql.mapping.reflection.ClassMapping;
import org.openntf.xsp.nosql.communication.driver.DominoConstants;
import org.openntf.xsp.nosql.communication.driver.impl.AbstractEntityConverter;
import org.openntf.xsp.nosql.mapping.extension.ItemFlags;
import org.openntf.xsp.nosql.mapping.extension.ItemStorage;

import com.hcl.domino.db.model.DateItem;
import com.hcl.domino.db.model.DateTimeItem;
import com.hcl.domino.db.model.Document;
import com.hcl.domino.db.model.Item;
import com.hcl.domino.db.model.NumberItem;
import com.hcl.domino.db.model.TextItem;
import com.hcl.domino.db.model.TimeItem;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.nosql.ServiceLoaderProvider;
import jakarta.nosql.ValueWriter;
import jakarta.nosql.document.DocumentEntity;

public class ProtonEntityConverter extends AbstractEntityConverter {

	private final Jsonb jsonb;
	
	public ProtonEntityConverter() {
		this.jsonb = JsonbBuilder.create();
	}
	
	public Stream<DocumentEntity> convertDocuments(String entityName, List<Document> docs, ClassMapping classMapping) {
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
	}
	
	public Document convertNoSQLEntity(DocumentEntity entity, boolean inserting, ClassMapping classMapping) {
		requireNonNull(entity, "entity is required"); //$NON-NLS-1$
		@SuppressWarnings("unchecked")
		List<ValueWriter<Object, Object>> writers = ServiceLoaderProvider.getSupplierStream(ValueWriter.class)
			.map(w -> (ValueWriter<Object, Object>)w)
			.collect(Collectors.toList());

		List<Item<?>> items = entity.getDocuments()
			.stream()
			.map(doc -> {
				// TODO attachment support
				
				if(!DominoConstants.SKIP_WRITING_FIELDS.contains(doc.getName())) {
					Optional<ItemStorage> optStorage = getFieldAnnotation(classMapping, doc.getName(), ItemStorage.class);
					// Check if we should skip processing
					if(optStorage.isPresent()) {
						ItemStorage storage = optStorage.get();
						if(!storage.insertable() && inserting) {
							return null;
						} else if(!storage.updatable() && !inserting) {
							return null;
						}
					}
					
					Object value = doc.get();
					if(value == null) {
						return new TextItem(doc.getName(), (String)null);
					} else {
						Object val = value;
						for(ValueWriter<Object, Object> w : writers) {
							if(w.test(value.getClass())) {
								val = w.write(value);
								break;
							}
						}
						
						// Check for a @ItemFlags annotation
						Optional<ItemFlags> itemFlagsOpt = getFieldAnnotation(classMapping, doc.getName(), ItemFlags.class);
						List<com.hcl.domino.db.model.ItemFlags> flags = new ArrayList<>();
						if(itemFlagsOpt.isPresent()) {
							ItemFlags itemFlags = itemFlagsOpt.get();
							
							if(!itemFlags.saveToDisk()) {
								// Best handled by removing the item
								return new TextItem(doc.getName(), (String)null);
							}
							
							if(itemFlags.authors()) {
								flags.add(com.hcl.domino.db.model.ItemFlags.ITEM_FLAG_AUTHORS);
							}
							if(itemFlags.readers()) {
								flags.add(com.hcl.domino.db.model.ItemFlags.ITEM_FLAG_READERS);
							}
							if(itemFlags.authors() || itemFlags.readers() || itemFlags.names()) {
								flags.add(com.hcl.domino.db.model.ItemFlags.ITEM_FLAG_NAMES);
							}
							if(itemFlags.encrypted()) {
								flags.add(com.hcl.domino.db.model.ItemFlags.ITEM_FLAG_ENCRYPT);
							}
							if(!itemFlags.summary()) {
								flags.add(com.hcl.domino.db.model.ItemFlags.ITEM_FLAG_NONSUMMARY);
							}
						}
						
						// Check if the item is expected to be stored specially, which may be handled down the line
						if(optStorage.isPresent() && optStorage.get().type() != ItemStorage.Type.Default) {
							ItemStorage storage = optStorage.get();
							switch(storage.type()) {
							case JSON:
								Object fVal = val;
								String json = AccessController.doPrivileged((PrivilegedAction<String>)() -> jsonb.toJson(fVal));
								flags.add(com.hcl.domino.db.model.ItemFlags.ITEM_FLAG_NONSUMMARY);
								return new TextItem(doc.getName(), json, flags.toArray(new com.hcl.domino.db.model.ItemFlags[flags.size()]));
							case MIME:
								throw new UnsupportedOperationException("MIME storage is unsupported");
							case MIMEBean:
								throw new UnsupportedOperationException("MIMEBean storage is unsupported");
							case Default:
							default:
								// Shouldn't get here
								throw new UnsupportedOperationException(MessageFormat.format("Unable to handle storage type {0}", storage.type()));
							}
						} else {
							Object dominoVal = val;
							
							// Set number precision if applicable
							if(optStorage.isPresent()) {
								int precision = optStorage.get().precision();
								if(precision > 0) {
									dominoVal = applyPrecision(dominoVal, precision);
								}
							}
							
							return toItem(doc.getName(), dominoVal, flags);
						}
					}
				}
				return null;
			})
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		
		items.add(new TextItem(DominoConstants.FIELD_NAME, entity.getName()));
		return new Document(items);
	}
	
	@SuppressWarnings("unchecked")
	private Item<?> toItem(String name, Object value, List<com.hcl.domino.db.model.ItemFlags> flags) {
		com.hcl.domino.db.model.ItemFlags[] flagsArray = flags.toArray(new com.hcl.domino.db.model.ItemFlags[flags.size()]);
		
		if(value instanceof Collection && ((Collection<?>)value).isEmpty()) {
			return new TextItem(name, (String)null, flagsArray);
		}
		
		if(value instanceof String) {
			return new TextItem(name, (String)value, flagsArray);
		} else if(value instanceof Collection && ((Collection<?>)value).iterator().next() instanceof String) {
			List<String> values = new ArrayList<>((Collection<String>)value);
			return new TextItem(name, values, flagsArray);
		} else if(value instanceof LocalDate) {
			return new DateItem(name, (LocalDate)value, flagsArray);
		} else if(value instanceof Collection && ((Collection<?>)value).iterator().next() instanceof LocalDate) {
			List<LocalDate> values = new ArrayList<>((Collection<LocalDate>)value);
			return new DateItem(name, values, false, flagsArray);
		} else if(value instanceof LocalTime) {
			return new TimeItem(name, (LocalTime)value, flagsArray);
		} else if(value instanceof Collection && ((Collection<?>)value).iterator().next() instanceof LocalTime) {
			List<LocalTime> values = new ArrayList<>((Collection<LocalTime>)value);
			return new TimeItem(name, values, false, flagsArray);
		} else if(value instanceof ZonedDateTime) {
			return new DateTimeItem(name, (ZonedDateTime)value, flagsArray);
		} else if(value instanceof Collection && ((Collection<?>)value).iterator().next() instanceof ZonedDateTime) {
			List<ZonedDateTime> values = new ArrayList<>((Collection<ZonedDateTime>)value);
			return new DateTimeItem(name, values, false, flagsArray);
		} else if(value instanceof Number) {
			return new NumberItem(name, (Number)value, flagsArray);
		} else if(value instanceof Collection && ((Collection<?>)value).iterator().next() instanceof Number) {
			List<Number> values = new ArrayList<>((Collection<Number>)value);
			return new NumberItem(name, values, false, flagsArray);
		}
		
		throw new UnsupportedOperationException(MessageFormat.format("Unable to convert value of type {0}", value.getClass().getName()));
	}
}
