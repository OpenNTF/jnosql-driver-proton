package org.openntf.xsp.nosql.communication.driver.proton.impl;

import org.openntf.xsp.nosql.communication.driver.proton.DatabaseSupplier;

import jakarta.enterprise.inject.spi.CDI;
import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfiguration;

public class ProtonDocumentConfiguration implements DocumentConfiguration {
	public static final String SETTING_SUPPLIER = "databaseSupplier"; //$NON-NLS-1$

	@SuppressWarnings("unchecked")
	@Override
	public ProtonDocumentCollectionManagerFactory get() {
		return new ProtonDocumentCollectionManagerFactory(
			CDI.current().select(DatabaseSupplier.class).get()
		);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ProtonDocumentCollectionManagerFactory get(Settings settings) {
		DatabaseSupplier supplier = settings.get(SETTING_SUPPLIER)
			.map(DatabaseSupplier.class::cast)
			.orElseGet(() -> CDI.current().select(DatabaseSupplier.class).get());
		return new ProtonDocumentCollectionManagerFactory(supplier);
	}

}
