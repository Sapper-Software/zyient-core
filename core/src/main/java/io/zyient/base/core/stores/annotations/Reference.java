/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.stores.annotations;

import io.zyient.base.common.GlobalConstants;
import io.zyient.base.common.model.entity.IEntity;

import javax.persistence.CascadeType;
import javax.persistence.JoinColumns;
import java.lang.annotation.*;

/**
 * Specify a reference entity to join with.
 * Reference entity can be any defined entity
 * that can be loaded and persisted by the entity manager.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
@SuppressWarnings("rawtypes")
public @interface Reference {
    /**
     * Target entity to JOIN with.
     *
     * @return - Target entity class.
     */
    Class<? extends IEntity<?>> target();

    /**
     * (Optional)
     * <p>
     * Specify the join type.
     * Default is One to One.
     *
     * @return - Join type.
     */
    EJoinType type() default EJoinType.One2One;

    /**
     * Column set to join on.
     *
     * @return - Join Columns
     */
    JoinColumns columns() default @JoinColumns(value = {});

    /**
     * (Optional) The operations that must be cascaded to
     * the target of the association.
     * <p> Defaults to no operations being cascaded.
     *
     * <p> When the target collection is a {@link java.util.Map
     * java.util.Map}, the <code>cascade</code> element applies to the
     * map value.
     */
    CascadeType[] cascade() default {};

    /**
     * (Optional) Whether to apply the remove operation to entities that have
     * been removed from the relationship and to cascade the remove operation to
     * those entities.
     *
     * @since Java Persistence 2.0
     */
    boolean orphanRemoval() default false;

    /**
     * (Optional)
     * Query to be applied to the join condition.
     *
     * @return - Query String.
     */
    String query() default GlobalConstants.EMPTY;

    /**
     * (Optional) Reference field for JSON
     * objects.
     *
     * @return
     */
    String reference() default GlobalConstants.EMPTY;
}
