package com.ajjpj.asqlmapper.demo.snippets;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Immutable implementation of {@link AbstractPerson}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code Person.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code Person.of()}.
 */
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@Immutable
@CheckReturnValue
public final class Person implements AbstractPerson {
  private final Long id;
  private final String name;

  private Person (Long id, String name) {
    this.id = Objects.requireNonNull(id, "id");
    this.name = Objects.requireNonNull(name, "name");
  }

  private Person (Person original, Long id, String name) {
    this.id = id;
    this.name = name;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPerson#id() id} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final Person withId(Long value) {
    Long newValue = Objects.requireNonNull(value, "id");
    if (this.id.equals(newValue)) return this;
    return new Person(this, newValue, this.name);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPerson#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final Person withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new Person(this, this.id, newValue);
  }

  /**
   * This instance is equal to all instances of {@code Person} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof Person
        && equalTo((Person) another);
  }

  private boolean equalTo(Person another) {
    return id.equals(another.id)
        && name.equals(another.name);
  }

  /**
   * Computes a hash code from attributes: {@code id}, {@code name}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + id.hashCode();
    h += (h << 5) + name.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Person} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Person")
        .omitNullValues()
        .add("id", id)
        .add("name", name)
        .toString();
  }

  /**
   * Construct a new immutable {@code Person} instance.
   * @param id The value for the {@code id} attribute
   * @param name The value for the {@code name} attribute
   * @return An immutable Person instance
   */
  public static Person of(Long id, String name) {
    return new Person(id, name);
  }

  /**
   * Creates an immutable copy of a {@link AbstractPerson} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Person instance
   */
  public static Person copyOf(AbstractPerson instance) {
    if (instance instanceof Person) {
      return (Person) instance;
    }
    return Person.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link Person Person}.
   * <pre>
   * Person.builder()
   *    .id(Long) // required {@link AbstractPerson#id() id}
   *    .name(String) // required {@link AbstractPerson#name() name}
   *    .build();
   * </pre>
   * @return A new Person builder
   */
  public static Person.Builder builder() {
    return new Person.Builder();
  }

  /**
   * Builds instances of type {@link Person Person}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private long initBits = 0x3L;

    private @Nullable
    Long id;
    private @Nullable
    String name;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractPerson} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder from(AbstractPerson instance) {
      Objects.requireNonNull(instance, "instance");
      id(instance.id());
      name(instance.name());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractPerson#id() id} attribute.
     * @param id The value for id
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder id(Long id) {
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractPerson#name() name} attribute.
     * @param name The value for name
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Builds a new {@link Person Person}.
     * @return An immutable instance of Person
     * @throws IllegalStateException if any required attributes are missing
     */
    public Person build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new Person(null, id, name);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ID) != 0) attributes.add("id");
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      return "Cannot build Person, some of required attributes are not set " + attributes;
    }
  }
}
