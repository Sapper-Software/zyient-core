/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.common.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.beans.BeanUtils;

import javax.annotation.Nonnull;

/**
 * Exception type to be used for escalating Validation errors.
 */
public class ValidationException extends Exception {
    private static final String __PREFIX__ = "Validation Error : %s";

    /**
     * Constructor with Error message.
     *
     * @param mesg - Error Message.
     */
    public ValidationException(String mesg) {
        super(String.format(__PREFIX__, mesg));
    }

    /**
     * Constructor with Error message and root cause.
     *
     * @param mesg  - Error Message.
     * @param cause - Cause.
     */
    public ValidationException(String mesg, Throwable cause) {
        super(String.format(__PREFIX__, mesg), cause);
    }

    /**
     * Constructor with root cause.
     *
     * @param cause - Cause.
     */
    public ValidationException(Throwable cause) {
        super(String.format(__PREFIX__, cause.getLocalizedMessage()), cause);
    }

    /**
     * Validate that a required property value is not NULL.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @throws ValidationException
     */
    public static void checkNotNull(@Nonnull String property, Object value)
            throws ValidationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (value == null) {
            throw new ValidationException(
                    String.format("Property value is NULL. [property=%s]",
                            property));
        }
    }

    /**
     * Validate that a required property value is not NULL.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @throws ValidationException
     */
    public static ValidationExceptions checkNotNull(@Nonnull String property, Object value,
                                                    ValidationExceptions errors) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (value == null) {
            ValidationException ve = new ValidationException(
                    String.format("Property value is NULL. [property=%s]",
                            property));
            errors = ValidationExceptions.add(ve, errors);
        }
        return errors;
    }

    /**
     * Validate a required property value is not NULL/Empty.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @throws ValidationException
     */
    public static void checkNotEmpty(@Nonnull String property, String value)
            throws ValidationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (Strings.isNullOrEmpty(value)) {
            throw new ValidationException(
                    String.format("Property value is NULL/Empty. [property=%s]",
                            property));
        }
    }

    /**
     * Validate a required property value is not NULL/Empty.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @param errors   - Handle for error wrapper.
     * @return - Handle for error wrapper.
     * @throws ValidationException
     */
    public static ValidationExceptions checkNotEmpty(@Nonnull String property,
                                                     String value,
                                                     ValidationExceptions errors) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (Strings.isNullOrEmpty(value)) {
            ValidationException ve = new ValidationException(
                    String.format("Property value is NULL/Empty. [property=%s]",
                            property));
            errors = ValidationExceptions.add(ve, errors);
        }
        return errors;
    }

    public static void check(@Nonnull Object entity,
                             @Nonnull String property) throws ValidationException {
        try {
            Object value = BeanUtils.getValue(entity, property);
            if (value == null) {
                throw new ValidationException(String.format("Property value is NULL. [type=%s][field=%s]",
                        entity.getClass().getCanonicalName(), property));
            }
            if (value instanceof String str) {
                if (Strings.isNullOrEmpty(str))
                    throw new ValidationException(String.format("Property value is empty. [type=%s][field=%s]",
                            entity.getClass().getCanonicalName(), property));
            }
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }
}