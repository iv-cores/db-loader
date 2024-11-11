package org.ivcode.dbloader.dialect


/**
 * Resolves the dialect of a driver.
 */
fun interface DialectResolver {

    /**
     * Resolves the dialect of the given driver.
     *
     * @param driver the driver to resolve
     * @return the dialect key
     * @throws IllegalArgumentException if the driver is not supported
     */
    fun resolve(driver: String): String
}
