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

import org.openntf.xsp.nosql.communication.driver.proton.AccessTokenSupplier;
import org.openntf.xsp.nosql.communication.driver.proton.DatabaseSupplier;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfiguration;

public class ProtonDocumentConfiguration implements DocumentConfiguration {
	public static final String SETTING_SUPPLIER = "databaseSupplier"; //$NON-NLS-1$
	public static final String SETTING_TOKENSUPPLIER = "accessTokenSupplier"; //$NON-NLS-1$

	@SuppressWarnings("unchecked")
	@Override
	public ProtonDocumentCollectionManagerFactory get() {
		Instance<AccessTokenSupplier> tokenInstance = CDI.current().select(AccessTokenSupplier.class);
		AccessTokenSupplier tokenSupplier = tokenInstance.isResolvable() ? tokenInstance.get() : () -> null;
		
		return new ProtonDocumentCollectionManagerFactory(
			CDI.current().select(DatabaseSupplier.class).get(),
			tokenSupplier
		);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ProtonDocumentCollectionManagerFactory get(Settings settings) {
		DatabaseSupplier supplier = settings.get(SETTING_SUPPLIER)
			.map(DatabaseSupplier.class::cast)
			.orElseGet(() -> CDI.current().select(DatabaseSupplier.class).get());
		AccessTokenSupplier tokenSupplier = settings.get(SETTING_TOKENSUPPLIER)
			.map(AccessTokenSupplier.class::cast)
			.orElseGet(() -> {
				Instance<AccessTokenSupplier> tokenInstance = CDI.current().select(AccessTokenSupplier.class);
				AccessTokenSupplier s = tokenInstance.isResolvable() ? tokenInstance.get() : () -> null;
				return s;
			});
		return new ProtonDocumentCollectionManagerFactory(supplier, tokenSupplier);
	}

}
