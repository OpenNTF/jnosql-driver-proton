/**
 * Copyright © 2022 Jakarta NoSQL Driver For Domino Via Proton Project
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

import org.openntf.xsp.nosql.communication.driver.proton.DatabaseSupplier;

import jakarta.nosql.document.DocumentCollectionManagerFactory;

public class ProtonDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory {
	
	private final DatabaseSupplier supplier;
	
	public ProtonDocumentCollectionManagerFactory(DatabaseSupplier supplier) {
		this.supplier = supplier;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ProtonDocumentCollectionManager get(String type) {
		return new ProtonDocumentCollectionManager(supplier);
	}

	@Override
	public void close() {

	}
}
