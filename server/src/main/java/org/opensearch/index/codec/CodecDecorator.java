/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.util.NamedSPILoader;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;
import java.util.Set;

/**
 * The {@link CodecDecorator} is the mechanism for plugins to customize the formats which are not
 * per-field but per-segment: the {@link CompoundFormat} and the {@link StoredFieldsFormat} (the
 * per-field formats should be contributed through {@link PerFieldCodecBuilder} instead). Any number
 * of plugins may contribute a decorator for the same index.
 *
 * <p>
 * In contrast to the per-field formats, whose names Apache Lucene records in the field attributes,
 * the per-segment formats are only reachable through the {@link org.apache.lucene.codecs.Codec} of
 * the segment. For this reason the decorators are identified by name: the names of the decorators
 * applied while writing a segment are recorded in the segment attributes and are resolved back,
 * through the service loader, while reading it (see also {@link DecoratedCodec}). Consequently:
 *
 * <ul>
 *   <li>every {@link CodecDecorator} implementation must be registered with the service loader
 *       ({@code META-INF/services/org.opensearch.index.codec.CodecDecorator}) and must provide a
 *       public no-arguments constructor: this is the instance which is going to be used to read the
 *       segments,</li>
 *   <li>the instance the plugin contributes for an index may carry index-level context (a mapper
 *       service, for example) but such context must only be needed for <b>writing</b>: the formats
 *       returned by the no-arguments instance have to be able to read any segment on their own,
 *       deriving whatever they need from the segment itself (its attributes and field infos),</li>
 *   <li>the name must be stable across the versions of the plugin, since it is persisted.</li>
 * </ul>
 *
 * <b>Note</b>: only one decorator per index may customize the {@link StoredFieldsFormat} (stored
 * fields are written row-wise, so, unlike the per-field formats, they could not be split between the
 * plugins), an attempt of two (or more) decorators to customize it leads to an exception.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class CodecDecorator implements NamedSPILoader.NamedSPI {
    /**
     * The holder of the service loader instance, initialized on demand (see also
     * {@link org.apache.lucene.codecs.Codec} which follows the very same pattern).
     */
    private static final class Holder {
        private static final NamedSPILoader<CodecDecorator> LOADER = new NamedSPILoader<>(CodecDecorator.class);

        private Holder() {}

        static NamedSPILoader<CodecDecorator> getLoader() {
            if (LOADER == null) {
                throw new IllegalStateException(
                    "The CodecDecorator instances could not be loaded: it is likely that the class is "
                        + "being initialized from the service loader itself"
                );
            }
            return LOADER;
        }
    }

    private final String name;

    protected CodecDecorator(String name) {
        NamedSPILoader.checkServiceName(name);
        this.name = name;
    }

    @Override
    public final String getName() {
        return name;
    }

    /**
     * Customizes the {@link CompoundFormat} of the segment, or returns {@link Optional#empty()} (the
     * default) if this decorator does not customize it. The decorators of all plugins are chained, ordered
     * by name, so the returned format must only alter the handling of the files this plugin owns and
     * delegate everything else to {@code current}.
     *
     * @param delegate the codec being decorated: the one of the index while writing, the one recorded in the
     *                 segment (resolved through the service loader) while reading
     * @param current the format to delegate to (the delegate's, possibly decorated by the preceding decorators)
     */
    public Optional<CompoundFormat> compoundFormat(Codec delegate, CompoundFormat current) {
        return Optional.empty();
    }

    /**
     * Customizes the {@link StoredFieldsFormat} of the segment, or returns {@link Optional#empty()}
     * (the default) if this decorator does not customize it. Only one decorator per index may do so.
     *
     * <p>
     * The whole delegate codec is provided, not only its stored fields format, since a format which derives
     * the stored fields from other structures of the segment (the derived source of {@code k-NN}, which reads
     * the vectors back) needs the delegate's per-field formats to open them: those resolve the formats of the
     * plugins from the field attributes, while reading as well.
     *
     * @param delegate the codec being decorated: the one of the index while writing, the one recorded in the
     *                 segment (resolved through the service loader) while reading
     */
    public Optional<StoredFieldsFormat> storedFieldsFormat(Codec delegate) {
        return Optional.empty();
    }

    /**
     * Looks up the decorator with the provided name through the service loader.
     */
    public static CodecDecorator forName(String name) {
        return Holder.getLoader().lookup(name);
    }

    /**
     * Returns the names of all decorators registered with the service loader.
     */
    public static Set<String> availableDecorators() {
        return Holder.getLoader().availableServices();
    }

    /**
     * Reloads the decorators from the provided class loader, so the ones contributed by the plugins
     * become visible (see also {@link org.apache.lucene.codecs.Codec#reloadCodecs}).
     */
    public static void reloadDecorators(ClassLoader classloader) {
        Holder.getLoader().reload(classloader);
    }
}
