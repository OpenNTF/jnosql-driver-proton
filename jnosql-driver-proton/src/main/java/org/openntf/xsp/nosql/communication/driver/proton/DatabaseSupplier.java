package org.openntf.xsp.nosql.communication.driver.proton;

import java.util.function.Supplier;

import com.hcl.domino.db.model.Database;

@FunctionalInterface
public interface DatabaseSupplier extends Supplier<Database> {

}
