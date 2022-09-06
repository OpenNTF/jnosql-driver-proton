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
